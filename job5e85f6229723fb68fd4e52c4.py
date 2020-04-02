import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e85f6229723fb68fd4e52c5','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	BostonHousingPricesRegression_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e85f6229723fb68fd4e52c5", spark, "{'url': '/Demo/BostonTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi0ef076722999cf4cd8859e9aafdb7b76', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e85f6229723fb68fd4e52c5','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e85f6229723fb68fd4e52c5','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e85f6229723fb68fd4e52c6','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	BostonHousingPricesRegression_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e85f6229723fb68fd4e52c5"],{"5e85f6229723fb68fd4e52c5": BostonHousingPricesRegression_DBFS}, "5e85f6229723fb68fd4e52c6", spark,json.dumps( {"FE": [{"feature": "CRIM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "4.71", "stddev": "11.7", "min": "0.01301", "max": "88.9762", "missing": "0"}, "transformation": ""}, {"feature": "ZN", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "9.08", "stddev": "18.96", "min": "0.0", "max": "100.0", "missing": "0"}, "transformation": ""}, {"feature": "INDUS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "11.24", "stddev": "6.62", "min": "0.46", "max": "27.74", "missing": "0"}, "transformation": ""}, {"feature": "CHAS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "0.06", "stddev": "0.25", "min": "0.0", "max": "1.0", "missing": "0"}, "transformation": ""}, {"feature": "NOX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "0.57", "stddev": "0.12", "min": "0.392", "max": "0.871", "missing": "0"}, "transformation": ""}, {"feature": "RM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "6.26", "stddev": "0.73", "min": "3.561", "max": "8.725", "missing": "0"}, "transformation": ""}, {"feature": "AGE", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "71.58", "stddev": "26.56", "min": "6.5", "max": "100.0", "missing": "0"}, "transformation": ""}, {"feature": "DIS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "3.71", "stddev": "2.07", "min": "1.3459", "max": "9.2229", "missing": "0"}, "transformation": ""}, {"feature": "RAD", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "10.08", "stddev": "8.95", "min": "1.0", "max": "24.0", "missing": "0"}, "transformation": ""}, {"feature": "TAX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "415.13", "stddev": "172.51", "min": "188.0", "max": "711.0", "missing": "0"}, "transformation": ""}, {"feature": "PTRATIO", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "18.31", "stddev": "2.11", "min": "13.0", "max": "21.0", "missing": "0"}, "transformation": ""}, {"feature": "B", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "349.07", "stddev": "103.57", "min": "0.32", "max": "396.9", "missing": "0"}, "transformation": ""}, {"feature": "LSTAT", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "13.36", "stddev": "7.29", "min": "2.87", "max": "34.02", "missing": "0"}, "transformation": ""}, {"feature": "MEDV", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "140", "mean": "21.56", "stddev": "9.39", "min": "5.0", "max": "50.0", "missing": "0"}, "transformation": ""}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e85f6229723fb68fd4e52c6','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e85f6229723fb68fd4e52c6','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e85f6229723fb68fd4e52c7','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	BostonHousingPricesRegression_AutoML = tpot_execution.Tpot_execution.run(["5e85f6229723fb68fd4e52c6"],{"5e85f6229723fb68fd4e52c6": BostonHousingPricesRegression_AutoFE}, "5e85f6229723fb68fd4e52c7", spark,json.dumps( {"model_type": "classification", "label": "MEDV", "features": ["CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT"], "percentage": "30", "executionTime": "5", "sampling": "0", "sampling_value": "none", "run_id": "", "model_id": "5e85f8999723fb68fd4e5346", "ProjectName": "ML Sample Problems", "PipelineName": "BostonHousingPricesRegression", "pipelineId": "5e85f6229723fb68fd4e52c4", "userid": "5df78f4be2f2eff24740bbd7", "runid": "", "url_ResultView": "http://13.68.212.36:3200", "experiment_id": "480623611921769"}))

	PipelineNotification.PipelineNotification().completed_notification('5e85f6229723fb68fd4e52c7','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e85f6229723fb68fd4e52c7','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)

