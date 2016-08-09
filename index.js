/**
 * Created by pavelnovotny on 02.08.16.
 */
var oracledb = require('oracledb');

oracledb.getConnection(
    {
        user          : "JRN_CORE",
        password      : "AM7__6_VCM91___",
        connectString : "jrn"
    },
    function(err, connection) {
        if (err) { console.error(err); return; }
        connection.execute(
            "SELECT cjd.jmscorrelationid, cjd.messageid, to_char(cjd.event_time, 'DD.MM.YYYY HH24:Mi:Ss') as event_time, to_char(cjd.db_time, 'DD.MM.YYYY HH24:Mi:Ss') as db_time, cjd.businessid_value as businessid, cjd.conversationid_value as conversationid, cje.id, cje.name, cjd.text_message \n" +
            "FROM cip_jrn_data cjd, cip_jrn_event cje where\n" +
            "cjd.text_message LIKE '%61090999%' and\n" +
            "cjd.event_id = cje.id and \n" +
            "cjd.db_time > to_date('07.07.2016 19:00', 'DD.MM.YYYY HH24:Mi') and \n" +
            "cjd.db_time < to_date('07.07.2016 19:59', 'DD.MM.YYYY HH24:Mi')\n" +
            "order by cjd.event_time desc",
        function(err, result)
            {
                if (err) { console.error(err); return; }
                console.log(result.rows);
            });
    });