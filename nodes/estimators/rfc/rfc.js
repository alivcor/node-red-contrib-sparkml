module.exports = function(RED){
	function randomForestClassifier(config){
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
			estimatorType: "rfc"
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("random forest classifier", randomForestClassifier)
}
