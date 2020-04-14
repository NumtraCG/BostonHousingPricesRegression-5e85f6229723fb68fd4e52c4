import json
import Connectors
import Transformations
import AutoML
try:
    BostonHousingPricesRegression_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e85f6229723fb68fd4e52c5", spark, "{'url': '/Demo/BostonTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi0ef076722999cf4cd8859e9aafdb7b76', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    BostonHousingPricesRegression_AutoFE = Transformations.TransformationMain.run(["5e85f6229723fb68fd4e52c5"], {"5e85f6229723fb68fd4e52c5": BostonHousingPricesRegression_DBFS}, "5e85f6229723fb68fd4e52c6", spark, json.dumps({"FE": [{"transformationsData": {}, "feature": "CRIM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "3.53", "stddev": "8.6", "min": "0.00632", "max": "88.9762", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "ZN", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "11.42", "stddev": "23.3", "min": "0.0", "max": "100.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "INDUS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "11.06", "stddev": "6.84", "min": "0.46", "max": "27.74", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "CHAS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "0.07", "stddev": "0.25", "min": "0.0", "max": "1.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "NOX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "0.55", "stddev": "0.12", "min": "0.385", "max": "0.871", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "RM", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "6.29", "stddev": "0.71", "min": "3.561", "max": "8.78", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "AGE", "type": "real", "selected": "True", "replaceby": "mean", "stats": {
                                                                                  "count": "420", "mean": "68.94", "stddev": "28.48", "min": "2.9", "max": "100.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "DIS", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "3.84", "stddev": "2.16", "min": "1.1296", "max": "12.1265", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "RAD", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "9.31", "stddev": "8.55", "min": "1.0", "max": "24.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "TAX", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "406.61", "stddev": "164.36", "min": "187.0", "max": "711.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "PTRATIO", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "18.38", "stddev": "2.2", "min": "12.6", "max": "22.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "B", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "357.59", "stddev": "89.73", "min": "0.32", "max": "396.9", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "LSTAT", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "12.73", "stddev": "7.29", "min": "1.73", "max": "37.97", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "MEDV", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "420", "mean": "22.65", "stddev": "9.31", "min": "5.0", "max": "50.0", "missing": "0"}, "transformation": ""}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionRegression(BostonHousingPricesRegression_AutoFE, [
                              "CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT"], "MEDV")

except Exception as ex:
    print(ex)
