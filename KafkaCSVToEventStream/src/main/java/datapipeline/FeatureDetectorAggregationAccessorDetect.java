package datapipeline;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.agg.access.AggregationState;
import com.espertech.esper.epl.agg.access.AggregationAccessor;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;

import java.util.Collection;

/*public class FeatureDetectorAggregationAccessorDetect implements AggregationAccessor {

    public Object getValue(AggregationState state, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        return ((FeatureDetectorAggregationState) state).getCycle();
    }

    public Collection<EventBean> getEnumerableEvents(AggregationState state, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        return null;
    }

    public EventBean getEnumerableEvent(AggregationState state, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        return null;
    }

    public Collection<Object> getEnumerableScalar(AggregationState state, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        return null;
    }


}*/
