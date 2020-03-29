module.exports = function(RED){
	function exportPredictionsAsCSV(config){
		const path = require('path')
		const utils = require('../../../utils/utils')

        var node = this;
        var flowContext = node.context().flow;

		//set configurations
		node.file = __dirname + '/predictions_csv.py'
		node.topic = 'sparkml'
		node.config = {
			csvPath: config.csvPath,
			exportColumns: config.exportColumns
		}

		utils.run(RED, node, config)
	}
	RED.nodes.registerType("export predictions as csv", exportPredictionsAsCSV)
}
