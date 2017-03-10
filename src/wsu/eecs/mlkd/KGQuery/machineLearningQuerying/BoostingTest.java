package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

import java.util.HashMap;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;

public class BoostingTest {

	public static void main(String[] args) throws XGBoostError {
	
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("eta", 0.3);
		params.put("booster", "gbtree");
		params.put("silent", 1);
		params.put("objective", "multi:softmax");
		params.put("num_class", "6"); // 0stop, 1,2,3,4,5
		params.put("eval_metric", "mlogloss");
		
		DMatrix dmat = new DMatrix("newClsExmpls_0.txt");
		
		HashMap<String, DMatrix> watches = new HashMap<String, DMatrix>();
		
		watches.put("selTrain", dmat);
		
		Booster boosterClassifier = XGBoost.train(dmat, params, 1, watches, null, null);
	}

}
