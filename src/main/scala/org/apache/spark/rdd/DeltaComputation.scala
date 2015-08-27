package org.apache.spark.rdd
// in this package so we can access package-private RDDs like MappedRDD

import edu.berkeley.cs.amplab.spark.indexedrdd._
import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by t-jamth on 8/12/2015.
 */

object DeltaComputation {
  // TODO make this code more robust, the field may not always be called "f"
  // this code is necessary because the "f" fields of these classes are private
  val mF = classOf[MappedRDD[_,_]].getDeclaredField("f")
  mF.setAccessible(true)
  val fF = classOf[FilteredRDD[_]].getDeclaredField("f")
  fF.setAccessible(true)
  val fmF = classOf[FlatMappedRDD[_,_]].getDeclaredField("f")
  fmF.setAccessible(true)
  val mvF = classOf[MappedValuesRDD[_,_,_]].getDeclaredField("org$apache$spark$rdd$MappedValuesRDD$$f")
  mvF.setAccessible(true)

  /**
   * @param source the full computation pipeline
   * @param stopPoint the first RDD in the pipeline, and the RDD for which we provide
   *                  positive and negative deltas
   *
   * source and stopPoint must both be RDD[(_, _)], source should have partitioner for good performance
   *
   * TODO source must also have partitioner for good performance, this should be fixed by adding
   * IndexedRDD functionality to just use input partitions during construction even if there is no
   * partitioner and just support scan and applyDeltas operations (assuming that input datasets
   * for applyDeltas are partitioned the same way)
   *
   * elements of source must have distinct keys (or IndexedRDD cannot be constructed)
   * (also true for stopPoint, but this is natural because positive and negative deltas are specified
   * by key)
   *
   * only support single-dependency RDD's (linear DAG), one-to-many record transformation or shuffles
   * (multi-dependency RDDs not supported because custom delta function required, also full-partition
   * operations like mapPartition not supported for the same reason)
   *
   * source, stopPoint, and post-shuffle datasets can only have String or Long keys (IndexedRDD limitation)
   */
  def newInstance[A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag](source: RDD[(A,B)],
    stopPoint: RDD[(C,D)]): DeltaComputation[A,B,C,D] = {
    val dc = new DeltaComputation(source, stopPoint, mutable.Map.empty[RDD[_],
      IndexedRDD[_,_]], null)
    dc.rewrite[A,B,C]
    dc
  }

  // removes each element of b from a
  private def performNegDelta[V: ClassTag](a: Iterable[V], b: Iterable[V]): Iterable[V] = {
    // count of each distinct element in a
    var sourceMap = mutable.Map.empty[V, Int]
    for (e <- a) {
      val newEntry = (e, sourceMap.getOrElse(e, 0) + 1)
      sourceMap += newEntry
    }
    // reduce counts based on elements of b
    for (e <- b) {
      val newEntry = (e, sourceMap(e) - 1)
      sourceMap += newEntry
    }
    var result = ArrayBuffer.empty[V]
    for (kv <- sourceMap) {
      // safe to use multiple references to the same element since this array will go into
      // an RDD and clients should not mutate RDDs
      result ++= List.fill(kv._2)(kv._1)
    }
    result
  }
}

// TODO generics somewhat abused throughout
class DeltaComputation[A: ClassTag, B: ClassTag, D: ClassTag, E: ClassTag] private (private val source: RDD[(A,B)],
                       private val stopPoint: RDD[(D,E)],
                       // map from input, output, and post-shuffle RDD's in the original DAG to the
                       // IndexedRDD's that store their contents for fast point lookup and update
                       private val indexedRdds: mutable.Map[RDD[_], IndexedRDD[_,_]],
                       private var transformed: IndexedRDD[A,B]) {

  implicit val anySer: KeySerializer[_] = new AnySerializer

  // client should work with this rewritten RDD
  def getTransformed: IndexedRDD[A,B] = {
    transformed
  }

  // construct a copy of this RDD with the potentially new set of dependencies given
  private def rebuildWithNewDeps[K: ClassTag, V: ClassTag, C: ClassTag](rdd: RDD[_],
    deps: Seq[Dependency[_]]): RDD[_] = {
    // TODO (minor) we can cache all of the fields fetched from the source RDD rather fetching
    // them again on every call to applyDeltas
    if (rdd.isInstanceOf[MappedRDD[K,V]]) {
      val mRdd = rdd.asInstanceOf[MappedRDD[K,V]]
      return new MappedRDD(deps(0).rdd.asInstanceOf[RDD[K]],
        DeltaComputation.mF.get(mRdd).asInstanceOf[K => V])
    } else if (rdd.isInstanceOf[FilteredRDD[K]]) {
      val fRdd = rdd.asInstanceOf[FilteredRDD[K]]
      return new FilteredRDD(deps(0).rdd.asInstanceOf[RDD[K]],
        DeltaComputation.fF.get(fRdd).asInstanceOf[K => Boolean])
    } else if (rdd.isInstanceOf[FlatMappedRDD[K,V]]) {
      val fmRdd = rdd.asInstanceOf[FlatMappedRDD[K,V]]
      return new FlatMappedRDD(deps(0).rdd.asInstanceOf[RDD[K]],
        DeltaComputation.fmF.get(fmRdd).asInstanceOf[K => TraversableOnce[V]])
    } else if (rdd.isInstanceOf[MappedValuesRDD[K,V,C]]) {
      val mvRdd = rdd.asInstanceOf[MappedValuesRDD[K,V,C]]
      return new MappedValuesRDD(deps(0).rdd.asInstanceOf[RDD[(K,V)]],
        DeltaComputation.mvF.get(mvRdd).asInstanceOf[V => C])
    } else if (rdd.isInstanceOf[ShuffledRDD[K,V,C]]) {
      val sRdd = rdd.asInstanceOf[ShuffledRDD[K,V,C]]
      val sDep = deps(0).asInstanceOf[ShuffleDependency[K,V,C]]
      return new ShuffledRDD(deps(0).rdd.asInstanceOf[RDD[(K,V)]],
        sDep.partitioner)
        .setSerializer(sDep.serializer.getOrElse(null))
        .setKeyOrdering(sDep.keyOrdering.getOrElse(null))
        .setAggregator(sDep.aggregator.getOrElse(null))
        .setMapSideCombine(sDep.mapSideCombine)
    } else {
      throw new IllegalArgumentException("Unexpected RDD of class " + rdd.getClass)
    }
  }

  // take a linear DAG and rebuilds it with IndexedRDD's inserted at the source, result,
  // and after shuffles
  // the client runs the initial computation through this modified pipeline (by working with
  // transformed) so that input, output, and intermediate (post-shuffle) state can be saved
  // for subsequent application of deltas to the pipeline
  // IndexedRDD's created in the rewrite and applyDeltas methods are always cached
  // because they are the state we will need in subsequent delta computations
  private def rewrite[K: ClassTag, V: ClassTag, C: ClassTag] = {
    implicit val ser = anySer.asInstanceOf[KeySerializer[A]]
    // putting output elements into IndexedRDD
    transformed = IndexedRDD(rewriteHelper[K,V,C](source).asInstanceOf[RDD[(A,B)]]).cache()
  }

  // recursive approach that rewrites the dependency of rdd and then wraps the result
  // in a copy of rdd, adding an IndexedRDD after this copy if necessary
  // TODO (minor) we need a map from rdd to rewritten result if the DAG is non linear (i.e.
  // may encounter a dependency multiple times)
  private def rewriteHelper[K: ClassTag, V: ClassTag, C: ClassTag](rdd: RDD[_]): RDD[_] = {
    implicit val ser = anySer.asInstanceOf[KeySerializer[K]]
    var hasShuffleDep = false
    if (rdd.dependencies.size > 1) {
      throw new IllegalArgumentException("Illegal multi-dependency RDD " + rdd.getClass)
    }
    // we don't need to go any further back than stopPoint since deltas will be applied here
    val newDeps = if (rdd.dependencies.isEmpty || rdd == stopPoint) null else rdd.dependencies(0) match {
      case n: OneToOneDependency[_] =>
        new OneToOneDependency(rewriteHelper[K,V,C](n.rdd))
      case s: ShuffleDependency[K,V,C] => {
        hasShuffleDep = true
        new ShuffleDependency(rewriteHelper[K,V,C](s.rdd).asInstanceOf[RDD[(K,V)]],
          s.partitioner, s.serializer, s.keyOrdering, s.aggregator, s.mapSideCombine)
      }
      case other => throw new IllegalArgumentException("Illegal dependency " + other.getClass
        + " for " + rdd.getClass)
    }
    if (hasShuffleDep) {
      val newEntry = (rdd, IndexedRDD(rebuildWithNewDeps[K,V,C](rdd, List(newDeps)).asInstanceOf[RDD[(K,V)]]).cache())
      indexedRdds += newEntry
      newEntry._2
    } else if (newDeps == null) {
      // at stopPoint, no need to rewrite it but add IndexedRDD after it to save it so that
      // we can look up the full records for negative deltas (see applyDeltasHelper)
      val newEntry = (rdd, IndexedRDD(rdd.asInstanceOf[RDD[(K,V)]]).cache()) // actually (D,E)
      indexedRdds += newEntry
      newEntry._2
    } else {
      rebuildWithNewDeps[K,V,C](rdd, List(newDeps))
    }
  }

  // rewrite source to pass these deltas through the computation pipeline, updating the
  // saved IndexedRDD state of this DeltaComputation along the way and producing a new
  // DeltaComputation with the updated state (including updated output at transformed)
  // pos deltas should contain full records, negative deltas only need to contain keys
  // (can use null or dummy values) since we have the records saved in an IndexedRDD from before
  def applyDeltas[K: ClassTag, V: ClassTag, C: ClassTag](pos: RDD[(D,E)], neg: RDD[(D,E)]): DeltaComputation[A,B,D,E] = {
    // applyDeltasHelper needs the RDD->IndexedRDD map from this DeltaComputation, so pass it in
    // this way
    val dc = new DeltaComputation(source, stopPoint, mutable.Map.empty ++ indexedRdds, null)
    // deltas passed through the computation pipeline, to be applied to this DeltaComputation's
    // final result
    val (posResult, negResult) = dc.applyDeltasHelper[K,V,C](source, pos, neg)
    // do update to previous final result and set transformed
    dc.transformed = transformed
      // first remove negative deltas then add positive, order is important because there
      // may be common keys between the two and we don't want the negative deltas to erase
      // the new positive ones
      .applyDelta(negResult.asInstanceOf[RDD[(A,B)]]){case (_, _) => null.asInstanceOf[B]}
      .applyDelta(posResult.asInstanceOf[RDD[(A,B)]]){case (_, _) => null.asInstanceOf[B]}.cache()
    dc
  }

  // recursive approach that passes pos and neg through dependency of rdd,
  // wrapping each result in a copy of rdd and updating IndexedRDD state
  // if we are at the input, after a shuffle, or at the output
  private def applyDeltasHelper[K: ClassTag, V: ClassTag, C: ClassTag](rdd: RDD[_], pos: RDD[(D,E)],
                                                                       neg: RDD[(D,E)]): (RDD[_], RDD[_]) = {
    // see inline comments in rewriteHelper for further insight
    var hasShuffleDep = false
    if (rdd.dependencies.size > 1) {
      throw new IllegalArgumentException("Illegal multi-dependency RDD " + rdd.getClass)
    }
    val newDeps = if (rdd.dependencies.isEmpty || rdd == stopPoint) null.asInstanceOf[(Dependency[_],Dependency[_])]
    else rdd.dependencies(0) match {
      case n: OneToOneDependency[_] =>
        val recurse = applyDeltasHelper[K,V,C](n.rdd, pos, neg)
        (new OneToOneDependency(recurse._1), new OneToOneDependency(recurse._2))
      case s: ShuffleDependency[K,V,C] => {
        hasShuffleDep = true
        val recurse = applyDeltasHelper[K,V,C](s.rdd, pos, neg)
        (new ShuffleDependency(recurse._1.asInstanceOf[RDD[(K,V)]],
          s.partitioner, s.serializer, s.keyOrdering, s.aggregator, s.mapSideCombine),
          new ShuffleDependency(recurse._2.asInstanceOf[RDD[(K,V)]],
            s.partitioner, s.serializer, s.keyOrdering, s.aggregator, s.mapSideCombine))
      }
      case other => throw new IllegalArgumentException("Illegal dependency " + other.getClass
        + " for " + rdd.getClass)
    }
    if (hasShuffleDep) {
      // newPos and newNeg are cached because they are used multiple times
      val newPos = rebuildWithNewDeps[K,V,C](rdd, List(newDeps._1)).asInstanceOf[RDD[(K, Iterable[V])]].cache()
      val newNeg = rebuildWithNewDeps[K,V,C](rdd, List(newDeps._2)).asInstanceOf[RDD[(K, Iterable[V])]].cache()
      // unique keys found in either newPos or newNeg
      val changedKeys = newPos.mapValues(v => null).zipPartitions(newNeg.mapValues(v => null), true)((a, b) => a ++ b)
        .reduceByKey((x, y) => x).cache()
      // update the corresponding post-shuffle state from the previous DeltaComputation
      val newEntry = (rdd, indexedRdds(rdd).asInstanceOf[IndexedRDD[K,Iterable[V]]]
        // functions specify how to merge values of keys that appear both in source and delta
        .applyDelta(newNeg){case (a, b) => DeltaComputation.performNegDelta(a, b)}
        .applyDelta(newPos){case (a, b) => a ++ b}.cache())
      // pos deltas are the new state's versions of the changed key-value pairs,
      // neg deltas are the old state's versions
      val retPair = (newEntry._2.select(changedKeys),
        indexedRdds(rdd).asInstanceOf[IndexedRDD[K,Iterable[V]]].select(changedKeys))
      indexedRdds += newEntry
      retPair
    } else if (newDeps == null) {
      // at stopPoint, so need to apply deltas to input IndexedRDD and then pass them on
      pos.cache()
      neg.cache()
      val newEntry = (rdd, indexedRdds(rdd).asInstanceOf[IndexedRDD[D, E]]
        // neg deltas should all be removed completely from input state and
        // pos deltas will be completely new keys, so no special merge functions required
        .applyDelta(neg){case (_, _) => null.asInstanceOf[E]}
        .applyDelta(pos){case (_, _) => null.asInstanceOf[E]}.cache())
      // must select the actual negative records from the the previous input IndexedRDD
      // since the negative deltas are generally just keys
      val retPair = (pos, indexedRdds(rdd).asInstanceOf[IndexedRDD[D, E]].select(neg))
      indexedRdds += newEntry
      retPair
    } else {
      (rebuildWithNewDeps[K,V,C](rdd, List(newDeps._1)), rebuildWithNewDeps[K,V,C](rdd, List(newDeps._2)))
    }
  }
}
