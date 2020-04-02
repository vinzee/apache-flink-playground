package com.state;

public class ManagedOperatorState {
//        implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
//    private final int threshold;
//private transient ListState<Tuple2<String, Integer>> checkpointedState;
//
//    private List<Tuple2<String, Integer>> bufferedElements;
//
//    public ManagedOperatorState(int threshold) {
//        this.threshold = threshold;
//        this.bufferedElements = new ArrayList<>();
//    }
//
//    @Override
//    public void invoke(Tuple2<String, Integer> value) throws Exception {
//        bufferedElements.add(value);
//        if (bufferedElements.size() == threshold) {
//            for (Tuple2<String, Integer> element : bufferedElements) {
//                // send it to the sink
//            }
//            bufferedElements.clear();
//        }
//    }
//
//    @Override
//    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//        checkpointedState.clear();
//        for (Tuple2<String, Integer> element : bufferedElements) {
//            checkpointedState.add(element);
//        }
//    }
//
//    @Override
//    public void initializeState(FunctionInitializationContext context) throws Exception {
//        ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<Tuple2<String, Integer>>("buffered-elements",
//                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
//                }));
//
//        checkpointedState = context.getOperatorStateStore().getListState(descriptor);       // .getUninonListState(descriptor)
//
//        if (context.isRestored()) {
//            for (Tuple2<String, Integer> element : checkpointedState.get()) {
//                bufferedElements.add(element);
//
//            }
//        }
//    }
}


