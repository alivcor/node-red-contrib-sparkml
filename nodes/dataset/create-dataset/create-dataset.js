module.exports = function(RED){
	function createDatasetNode(config){
		const path = require('path')
		const utils = require('../../../utils/utils')

		var node = this;

		//set configurations
		node.file = __dirname + '/create-dataset.py'
		node.topic = 'sparkml'
		node.config = {
			scheme: config.scheme,
			path: config.path,
			sparkMaster: config.sparkMaster,
			sparkApp: config.sparkApp,
			fileType: config.fileType,
			save: config.save,
			input: utils.listOfInt(config.input) || [0],
			output: parseInt(config.output) || undefined,
			trainingPartition: (Number(config.trainingPartition) || 80.0) / 100.0,
			separator: config.separator,
			shuffle: Boolean(config.shuffle),
			inferSchema: Boolean(config.inferSchema),
			readHeaders: Boolean(config.readHeaders),
            partitionCol: config.partitionCol,
			seed: parseInt(config.seed) || 1234
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("create sparkml dataset", createDatasetNode)
}
