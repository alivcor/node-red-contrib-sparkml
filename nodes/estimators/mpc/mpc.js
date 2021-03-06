module.exports = function(RED){
	function multilayerPerceptron(config){
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
			layers: config.layers,
			blockSize: config.blockSize,
			seed: config.seed,
			estimatorType: "mpc"
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("multilayer perceptron", multilayerPerceptron)
}
