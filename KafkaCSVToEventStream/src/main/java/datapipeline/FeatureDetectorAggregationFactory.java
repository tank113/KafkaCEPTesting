package datapipeline;


import com.espertech.esper.epl.agg.access.AggregationAccessorForge;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.plugin.*;
import com.espertech.esper.epl.expression.core.ExprEvaluator;

/*public class FeatureDetectorAggregationFactory implements PlugInAggregationMultiFunctionFactory{

    private ExprNode fromExpression;
    private ExprNode toExpression;

    public void addAggregationFunction(PlugInAggregationMultiFunctionDeclarationContext declarationContext) {
        // provides an opportunity to inspect where used
    }

    public PlugInAggregationMultiFunctionHandler validateGetHandler(PlugInAggregationMultiFunctionValidationContext validationContext) {
        if (validationContext.getParameterExpressions().length == 2) {
            fromExpression = validationContext.getParameterExpressions()[0];
            toExpression = validationContext.getParameterExpressions()[1];
        }
        return new FeatureDetectorAggregationHandler(this, validationContext) {
            @Override
            public AggregationAccessorForge getAccessorForge() {
                return null;
            }

            @Override
            public PlugInAggregationMultiFunctionStateForge getStateForge() {
                return null;
            }
        };
    }

    public ExprNode getToExpression() {
        return toExpression;
    }

    public ExprNode getFromExpression() {
        return fromExpression;
    }
}*/
