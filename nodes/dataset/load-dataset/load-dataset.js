module.exports = function(RED){
	function loadDatasetNode(config){
		const path = require('path')
		const utils = require('../../../utils/utils')

		var node = this;

		//set configurations
		node.file = __dirname + '/load-dataset.py'
		node.topic = 'sparkml'
		node.config = {
			// path: config.path,
			// input: Boolean(config.input),
			// output: Boolean(config.output)
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("load sparkml dataset", loadDatasetNode)
}
