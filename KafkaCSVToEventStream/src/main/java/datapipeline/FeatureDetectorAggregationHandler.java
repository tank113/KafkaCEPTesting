package datapipeline;

import com.espertech.esper.epl.agg.access.AggregationAccessor;
import com.espertech.esper.epl.agg.access.AggregationAgentForge;
import com.espertech.esper.epl.agg.access.AggregationStateKey;
import com.espertech.esper.epl.rettype.EPType;
import com.espertech.esper.epl.rettype.EPTypeHelper;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionAgentContext;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionHandler;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionStateFactory;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionValidationContext;

/*public abstract class FeatureDetectorAggregationHandler implements PlugInAggregationMultiFunctionHandler{

    private static final AggregationStateKey CYCLE_KEY = new AggregationStateKey() {};

    private final FeatureDetectorAggregationFactory factory;
    private final PlugInAggregationMultiFunctionValidationContext validationContext;

    public FeatureDetectorAggregationHandler(FeatureDetectorAggregationFactory factory, PlugInAggregationMultiFunctionValidationContext validationContext) {
        this.factory = factory;
        this.validationContext = validationContext;
    }

    public AggregationStateKey getAggregationStateUniqueKey() {
        return CYCLE_KEY;   // Share the same provider
    }

    public PlugInAggregationMultiFunctionStateFactory getStateFactory() {
        return new FeatureDetectorAggregationStateFactory(factory.getFromExpression(), factory.getToExpression());
    }

    public AggregationAccessor getAccessor() {
        if (validationContext.getFunctionName().toLowerCase().equals(FeatureDetectorConstant.FEATUREOUTPUT_NAME)) {
            return new FeatureDetectorAggregationAccessorDetect();
        }
        return new FeatureDetectorAggregationAccessorDetect();
    }

    public EPType getReturnType() {
        if (validationContext.getFunctionName().toLowerCase().equals(FeatureDetectorConstant.FEATUREOUTPUT_NAME)) {
            return EPTypeHelper.collectionOfSingleValue(factory.getFromExpression().getClass());
        }
        return EPTypeHelper.singleValue(Boolean.class) ;
    }

    public AggregationAgentForge getAggregationAgent(PlugInAggregationMultiFunctionAgentContext agentContext) {
        return null;
    }
}*/
