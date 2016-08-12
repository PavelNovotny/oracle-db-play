/**
 * Created by pavelnovotny on 02.08.16.
 */

var async = require('async');
var Client = require('node-rest-client').Client;
var client = new Client();
var oracledb = require('oracledb');
var productFetchCount = 10000; //počet řádků pro jednu iteraci čtení produktu
var maxServiceRowsInBulk = 1000; //max počet service v jednom bulku pro elastic, bulků je samozřejmě tolik, dokud se nevyčerpají data
var maxCursors = 100; //max počet cursorů na db spojení


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
            insertServices(resultSet, dbConnection, function(err) {
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

function processServiceChild(connection, servicePointId, callback) {
    async.waterfall([
        function(callback) {
            selectProduct(connection, servicePointId, function(err, resultSet) {
                callback(err, resultSet);
            });
        },
        function(resultSet, callback){
            insertProduct(resultSet, function(err, data) {
                callback(err, data);
            });
        }
    ], function (err, data) {
        if (err) return callback(err);
        return callback(null, data);
    });
}

function connect(callback) {
    oracledb.getConnection(
        {
            user          : "rp",
            password      : "*****",
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
            callback(null, result.resultSet);
        }
    );
}

function insertServices(resultSet, connection, callback) {
    var rowCount = maxServiceRowsInBulk;
    async.whilst(
        function() { return rowCount === maxServiceRowsInBulk; },
        function(callback) {
            resultSet.getRows( maxServiceRowsInBulk, function (err, rows) {
                if (err) return callback(err);
                rowCount = rows.length;
                processServiceRows(rows, connection, function(err, postData) {
                    if (err) return callback(err);
                    insertElasticBulkData(postData, function(err) { //todo in limit parallel
                        if (err) return callback(err);
                        callback(null);
                    });
                });
            });
        },
        function (err) {
            if (err) return callback(err);
            resultSet.close(function(err) {
                if (err) return callback(err);
                callback(null);
            });
        }
    );
}

function insertProduct(resultSet, callback) {
    var stringData="";
    var rowCount = productFetchCount;
    async.whilst(
        function() { return rowCount === productFetchCount; },
        function(callback) {
            resultSet.getRows( productFetchCount, function (err, rows) {
                if (err) return callback(err);
                rowCount = rows.length;
                processProductRows(rows, function(err, data) {
                    if (err) return callback(err);
                    stringData = stringData + data;
                    callback(null, stringData);
                });
            });
        },
        function (err, data) {
            if (err) return callback(err);
            resultSet.close(function(err) {
                if (err) return callback(err);
                callback(null, data);
            });
        }
    );



}

function processProductRows(rows, callback) {
    var postData = "";
    var product = null;
    var parameters = [];
    var parameter = null;
    if (rows.length > 0) {
        parameter = rows[0][4];
        postData = createProductHeader(rows[0]);
        product = rows[0][1];
    }
    for (var i=0; i< rows.length; i++) {
        if(rows[i][1] != product ) { //new product, create header
            product = rows[i][1];
            if (parameters.length > 0) { //přidány záznamy
                postData = postData + createProductData(rows[i], parameters);
            }
            parameters = [];
        }
        parameters.push({paramName :rows[i][4], paramValue:rows[i][5]});
    }
    if (rows.length > 0) {
        postData = postData + "\n"+ createProductData(rows[rows.length-1], parameters) + "\n";
    }
    return callback(null, postData);
}

function createProductData(row, parameters) {
    var rowData = {};
    rowData.productCode = row[1];
    rowData.name = row[2];
    rowData.status= row[3];
    //todo, v selectu Radovana BSCS8_CO_ID chybělo
    rowData.BSCS8_CO_ID= row[3];
    rowData.parameter= [];
    for (var i = 0; i < parameters.length; i++) {
        rowData.parameter.push(parameters[i]);
    }
    return JSON.stringify(rowData);
}

function createProductHeader(row) {
    var rowHeader = {"index":{}};
    rowHeader.index._index = "service_index";
    rowHeader.index._type ="product";
    rowHeader.index._id = row[0] + "_" +row[1];
    rowHeader.index._parent = row[0];
    return JSON.stringify(rowHeader);
}

function processProductRow(row) {
    var rowHeader = {"index":{}};
    rowHeader.index._index = "service_index";
    rowHeader.index._type ="product";
    rowHeader.index._id = row[0] + "_" +row[1];
    rowHeader.index._parent = row[0];
    var rowData = {};
    rowData.productCode = row[1];
    rowData.name = row[2];
    rowData.status= row[3];
    //todo, v selectu Radovana BSCS8_CO_ID chybělo
    rowData.BSCS8_CO_ID= row[3];
    rowData.parameter= [{paramName:row[4], paramValue:row[5]}];
    return JSON.stringify(rowHeader) + "\n" + JSON.stringify(rowData) + "\n";
}


function processServiceRows(rows, connection, callback) {
    var postData = "";
    // //todo limit maxCursors
    // for (var i=0; i< rows.length; i++) {
    //     processServiceChild(connection, rows[i][0], function(err, data) {
    //         postData = postData + processServiceRow(rows[i]);
    //         postData = postData + data;
    //     });
    // }
    // callback(null, postData);

    var q = async.queue(function(rowObject, callback) {
        processServiceChild(connection, rowObject.row[0], function(err, data) {
            if (err) return callback(err);
            postData = postData + processServiceRow(rowObject.row);
            postData = postData + data;
            callback();
        });
    }, maxCursors);

    q.drain = function() {
        callback(null, postData);
    };

    for (var i=0; i< rows.length; i++) {
        q.push({row : rows[i]}, function(err) {
            if (err) return callback(err);
        });
    }


}

function processServiceRow(row) {
    var rowHeader = {"index":{}};
    rowHeader.index._index = "service_index";
    rowHeader.index._type ="service";
    rowHeader.index._id = row[0];
    var rowData = {};
    rowData.datum ="2016-02-02";
    rowData.servicePointID = row[0];
    rowData.refCustNumber = row[1];
    rowData.IMSI= row[2];
    rowData.billingCycle= row[4];
    rowData.tarif= row[5];
    return JSON.stringify(rowHeader) + "\n" + JSON.stringify(rowData) + "\n";
}


function insertElasticBulkData(data, callback) {
    var args = {
        data: data
    };
    console.log(data);
    client.post("http://localhost:9200/_bulk", args, function (data, response) {
        callback(null);
        //console.log(data);
    });
}





