package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses);
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        assert records != null;
        List <Record> recordList = new ArrayList<>();
        while (records.hasNext()) {
            recordList.add(records.next());
        }
        Collections.sort(recordList, this.comparator);
        Run result = makeRun();
        result.addAll(recordList);
        return result;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement

        if (runs == null) {
            return null;
        }

        Run result = makeRun();
        PriorityQueue<Pair<Record, Integer>> priorityQ = new PriorityQueue<>(new RecordPairComparator());

        // First round: add one record from each run to priority queue
        List<List<Record>> allRunsList = new ArrayList<>();
        for (int i = 0; i < runs.size(); i++) {
            // Create list over iterator elements
            Run thisRun = runs.get(i);
            Iterator<Record> thisRunIter = thisRun.iterator();
            List<Record> thisRunList = new ArrayList<>();
            thisRunIter.forEachRemaining(thisRunList::add);
            allRunsList.add(thisRunList);
            //Add first element to priorityQ
            priorityQ.add(new Pair<> (thisRunList.remove(0), i));
        }

        // In each iteration we (1) add min element to result and (2) add another element into priorityQ
        // For (2), we first try add the next Record from the same Run that produced the most recent priorityQ poll.
        // If there are no more Records left in that run, we simply find the first non-empty run and add an element there.
        int nextAdd;
        while (!priorityQ.isEmpty()) {
            Pair<Record, Integer> minQelem = priorityQ.poll();
            if (minQelem != null) {
                result.add(minQelem.getFirst());
                nextAdd = minQelem.getSecond();
                List <Record> targetList = allRunsList.get(nextAdd);
                if (targetList.size() > 0) {
                    priorityQ.add(new Pair<> (targetList.remove(0), nextAdd));
                } else {
                    //Find first non-empty run and add element to priorityQ
                    for (int i = 0; i < allRunsList.size(); i++) {
                        List<Record> thisRunList = allRunsList.get(i);
                        if (thisRunList.size() > 0) {
                            priorityQ.add(new Pair<> (thisRunList.remove(0), i));
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement

        if (runs == null) {
            return null;
        }

        List<Run> result = new ArrayList<>();
        // In each iteration, merge (B - 1) runs
        for (int i = 0; i < runs.size(); i += this.numBuffers - 1) {
            int end = Math.min(runs.size(), i + this.numBuffers - 1);
            List<Run> setOfRuns = runs.subList(i, end);
            result.add(mergeSortedRuns(setOfRuns));
        }
        return result;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();
        // TODO(proj3_part1): implement

        //Pass 0: create sorted runs of length B
        ArrayList<Run> Runs0 = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            BacktrackingIterator<Record> blockIter = getBlockIterator(sourceIterator, this.getSchema(), this.numBuffers);
            Runs0.add(sortRun(blockIter));
        }

        //Pass 1- end: merge runs until we have one final run remaining
        List<Run> result = Runs0;
        while (result.size() > 1) {
            result = mergePass(result);
        }

        return result.size() > 0 ? result.get(0) : makeRun();
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

