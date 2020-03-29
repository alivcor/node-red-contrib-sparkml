module.exports = function(RED){
	function logisticRegression(config){
		const path = require('path')
		const utils = require('../../../utils/utils')

		var node = this;

		//set configurations
		node.file = __dirname + '/../estimators.py'
		node.topic = 'sparkml'
		node.config = {
			// path: config.path,
			labelCol: config.labelCol,
			featuresCol: config.featuresCol,
			maxIter: config.maxIter,
			regParam: config.regParam,
			elasticNetParam: config.elasticNetParam,
			estimatorType: "lr"
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("logistic regression", logisticRegression)
}
