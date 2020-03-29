module.exports = function(RED){
	function sqlTransform(config){
		const path = require('path')
		const utils = require('../../../utils/utils')

		var node = this;

		//set configurations
		node.file = __dirname + '/../transformers.py'
		node.topic = 'sparkml'
		node.config = {
			// path: config.path,
			statement: config.statement,
			transformerType: "sql"
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("sql transformer", sqlTransform)
}
