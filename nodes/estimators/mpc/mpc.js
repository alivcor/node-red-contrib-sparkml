module.exports = function(RED){
	function vectorAssembler(config){
		const path = require('path')
		const utils = require('../../../utils/utils')

		var node = this;

		//set configurations
		node.file = __dirname + '/../transformers.py'
		node.topic = 'sparkml'
		node.config = {
			// path: config.path,
			inputCols: config.inputCols,
			outputCol: config.outputCol,
			transformerType: "va"
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("vector assembler", vectorAssembler)
}
