/**
 * Created by pavelnovotny on 02.08.16.
 */

var async = require('async');
var Client = require('node-rest-client').Client;
var client = new Client();
var oracledb = require('oracledb');
var maxProductRowsForService = 100000;
var maxServiceRowsInBulk = 10000;

readData();

function readData() {
    var dbConnection;
    async.waterfall([
        function(callback) {
            connect(function(err, connection) {
                callback(err, connection);
            });
        },
        function(connection, callback) {
            dbConnection = connection;
            selectServices(connection, function(err, result) {
                callback(err, result.resultSet);
            });
        },
        function(resultSet, callback) {
            insertServices(resultSet, function(err) {
                callback(err);
            });
        }
    ], function (err) {
        if (err) return console.error(err);
        disconnect(dbConnection, function(err) {
            if (err) return console.error(err);
            console.log("Finished");
        });
    });
}

function processService(connection, servicePointId) {
    async.waterfall([
        function(callback) {
            selectProduct(connection, servicePointId, function(err, result) {
                callback(err, result, connection);
            });
        },
        function(result, callback){
            insertProduct(result, connection, function(err) {
                callback(err, connection);
            });
        }
    ], function (err, result) {
        if (err) return console.error(err);
        console.log(result);
    });
}

function connect(callback) {
    oracledb.getConnection(
        {
            user          : "rp",
            password      : "zavojnatka",
            connectString : "bscs1"
        },
        function(err, connection) {
            if (err) return callback (err);
            callback(null, connection);
        });
}

function disconnect(connection, callback) {
    connection.close(
        function(error) {
            if (error) return callback(error);
            callback(null);
        }
    );
}

function selectServices(connection, callback) {
    connection.execute(
        "select SERVICE_POINT_ID,\n" +
        "PAYER_REF_NUM,\n" +
        "IMSI,\n" +
        "MSISDN,\n" +
        "STATUS,\n" +
        "TARIFF,\n" +
        "BSCS8_CO_ID\n" +
        "from rp.postpaid_contracts",
        [],
        {resultSet: true},
        function (err, result) {
            if (err) callback(err);
            callback(null, result);
        });
}

function insertServices(resultSet, callback) {
    var rowCount = maxServiceRowsInBulk;
    async.whilst(
        function() { return rowCount === maxServiceRowsInBulk; },
        function(callback) {
            resultSet.getRows( maxServiceRowsInBulk, function (err, rows) {
                if (err) return callback(err);
                rowCount = rows.length;
                processServiceRows(rows, function(err) {
                    if (err) return callback(err);
                    callback(null);
                });
            });
        },
        function (err) {
            resultSet.close();
            if (err) return callback(err);
            callback(null);
        }
    );
}

function processServiceRows(rows, callback) {
    var data = "";
    for (var i=0; i< rows.length; i++) {
        var rowHeader = {"index":{}};
        rowHeader.index._index = "service_index";
        rowHeader.index._type ="service";
        rowHeader.index._id = rows[i][0];
        var rowData = {};
        rowData.datum ="2016-02-02";
        rowData.servicePointID = rows[i][0];
        rowData.refCustNumber = rows[i][1];
        rowData.IMSI= rows[i][2];
        rowData.billingCycle= rows[i][4];
        rowData.tarif= rows[i][5];
        data = data + JSON.stringify(rowHeader) + "\n" + JSON.stringify(rowData) + "\n";
        //todo processService
    }
    insertBulkData(data); //parallel execution
    
    callback(null);
}

function insertBulkData(data) {
    var args = {
        data: data
    };
    console.log(data);
    client.post("http://localhost:9200/_bulk", args, function (data, response) {
        //console.log(data);
    });
}



function insertProduct(result, connection, callback) {
    var resultSet = result.resultSet;
    var data = "";
    resultSet.getRows( maxProductRowsForService, function (err, rows) {
        if (err) return console.error(err);
        for (var i=0; i<rows.length; i++) {
            console.log(rows[i][0]);
            console.log(rows[i][1]);
            console.log(rows[i][2]);
            console.log(rows[i][3]);
            console.log(rows[i][4]);
            console.log(rows[i][5]);
            //var rowHeader = {"index":{}};
            // rowHeader.index._index = "service_index";
            // rowHeader.index._type ="service";
            // rowHeader.index._id = rows[i][0];
            // var rowData = {};
            // rowData.datum ="2016-02-02";
            // rowData.servicePointID = rows[i][0];
            // rowData.refCustNumber = rows[i][1];
            // rowData.IMSI= rows[i][2];
            // rowData.billingCycle= rows[i][4];
            // rowData.tarif= rows[i][5];
            // console.log(rowHeader);
            // console.log(rowData);
            // data = data + JSON.stringify(rowHeader) + "\n" + JSON.stringify(rowData) + "\n";
        }
        callback(null);
    });
}


function selectProduct(connection, servicePointId, callback) {
    var select = "SELECT \n" +
        "c.SERVICE_POINT_ID,\n" +
    "s.service_code,\n" +
    "s.service_name,\n" +
    "s.service_status,\n" +
    "p.PARAMETER_NAME,\n" +
    "p.PARAMETER_VALUE\n" +
    "  FROM rp.postpaid_contracts c, rp.postpaid_co_services s, rp.postpaid_co_services_params p\n" +
    " WHERE c.service_point_id = s.service_point_id\n" +
    "   AND s.service_point_id = p.service_point_id\n" +
    "   AND c.service_point_id = '" + servicePointId + "'\n" +
    "   AND s.service_code = p.service_code\n" +
    "order by s.SERVICE_CODE, p.PARAMETER_NAME"
    connection.execute( select, [], {resultSet: true},
        function (err, result) {
            if (err) return callback(err);
            callback(null, result);
        }
    );
}