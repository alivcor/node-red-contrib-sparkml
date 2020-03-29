module.exports = function(RED){
	function vectorIndexer(config){
		const path = require('path')
		const utils = require('../../../utils/utils')

		var node = this;

		//set configurations
		node.file = __dirname + '/../transformers.py'
		node.topic = 'sparkml'
		node.config = {
			// path: config.path,
			inputCol: config.inputCol,
			outputCol: config.outputCol,
			maxCategories: config.maxCategories,
			transformerType: "vi"
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("vector indexer", vectorIndexer)
}
