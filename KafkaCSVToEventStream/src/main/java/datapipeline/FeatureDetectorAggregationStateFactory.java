package datapipeline;

import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionStateFactory;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionStateContext;
import com.espertech.esper.epl.agg.access.AggregationState;
import com.espertech.esper.epl.expression.core.ExprEvaluator;

/*public class FeatureDetectorAggregationStateFactory implements PlugInAggregationMultiFunctionStateFactory {

    private final ExprNode fromEvaluator;
    private final ExprNode toEvaluator;

    public FeatureDetectorAggregationStateFactory(ExprNode fromEvaluator, ExprNode toEvaluator) {
        this.fromEvaluator = fromEvaluator;
        this.toEvaluator = toEvaluator;
    }

    public AggregationState makeAggregationState(PlugInAggregationMultiFunctionStateContext stateContext) {
        return new FeatureDetectorAggregationState(this);
    }

    public ExprNode getFromEvaluator() {
        return fromEvaluator;
    }

    public ExprNode getToEvaluator() {
        return toEvaluator;
    }
}*/
