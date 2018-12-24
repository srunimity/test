let util = require('util');
var cfg = require('config');
let LOG_CATEGORY = "planService";
let Q = require('q');
var client = require('../../config/elastic');
var logger = require('../../lib/logger');
const sequlizeConn = require('../../config/sequlize.connection');
const _ = require('lodash');
var esDao = require('./plan.esDao');
const validation = require('../../common_utils/validation');
const orderlineSchema = require('../../models/orderline.model');
const manifestSchema = require('../../models/manifest.model');
const manifestErrorLogSchema = require('../../models/manifestErrorLog.model');
const Sequelize = require('sequelize');
const Op = Sequelize.Op;
/**
 * This function is to get unplanned orders list
 * @param {number} from Page number for pagination
 * @param {number} size List size of each page for pagination
 */
var getUnplannedOrdersList = async (from, size) => {
    let logMethod = 'getUnplannedOrdersList';
    try {
        const response = await client.search({
            index: '' + cfg.indxName.unplannedOrdersINDX + '',
            from: from,
            size: size,
            type: 'doc',
            body: {
                query: {
                    "match": {
                        order_STTS: cfg.statusA
                    }
                }
            }
        });
        return {
            "orders": _.map(response.hits.hits, '_source')
        };
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw err;
    }
}

let updateOrderDetails = function (orderId, body) {
    let logMethod = "updateOrderDetails";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    let errMsg = "";

    let params = {
        orderId: orderId,
        orderLineId: body.order_LN_ID,
        pickupStartTime: body.order_LN_PCKUP_STRT_TM,
        pickupEndTime: body.order_LN_PCKUP_END_TM,
        deliveryStartTime: body.order_LN_DLVRY_STRT_TM,
        deliveryEndTime: body.order_LN_DLVRY_END_TM,
        weight: body.weight,
        palletCount: body.order_LN_PLET_CNT,
        commodityId: body.cmdty_ID,
        dispatchComment: body.order_LN_DLVRY_CMNT,
        dispatchInstructions: body.order_LN_DLVRY_INSTR,
        maxTemp: body.order_LN_MAX_TEMP,
        minTemp: body.order_LN_MIN_TEMP,
        priority: body.order_PRTY_TYPE,
        revenueMiles: body.rvnu_MI,
        revenueQuantity: body.rvnu_QTY,
        updateDate: new Date().toLocaleString(),
        updateUserId: 9999
    }
    //Need to remove UserId from above json.
    esDao.esGetUnplannedOrder(params.orderLineId).then(function (orderResult) {
        let unplannedorderdetails = orderResult[0];
        sequlizeConn.getMySQLConn().then((dbConnection) => {
            dbConnection.transaction(function (t) {
                let transObj = {
                    transaction: t
                };
                return esDao.esUpdateOrderDetails(params, transObj).then(function (result) {
                    return _dbUpdateOrderDetails(params, dbConnection, transObj).then(function (res) {
                        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                        t.commit();
                        deferred.resolve("Order details updated successfully.");

                    }).catch(function (err) {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        t.rollback();
                        deferred.reject(err);
                    });
                }).catch(function (err) {
                    logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));

                    //Revert back es update ;
                    let existingParams = {
                        orderId: unplannedorderdetails.order_ID,
                        orderLineId: unplannedorderdetails.order_LN_ID,
                        pickupStartTime: unplannedorderdetails.order_LN_PCKUP_STRT_TM,
                        pickupEndTime: unplannedorderdetails.order_LN_PCKUP_END_TM,
                        deliveryStartTime: unplannedorderdetails.order_LN_DLVRY_STRT_TM,
                        deliveryEndTime: unplannedorderdetails.order_LN_DLVRY_END_TM,
                        weight: unplannedorderdetails.weight,
                        palletCount: unplannedorderdetails.order_LN_PLET_CNT,
                        commodityId: unplannedorderdetails.cmdty_ID,
                        dispatchComment: unplannedorderdetails.order_LN_DLVRY_CMNT,
                        dispatchInstructions: unplannedorderdetails.order_LN_DLVRY_INSTR,
                        maxTemp: unplannedorderdetails.order_LN_MAX_TEMP,
                        minTemp: unplannedorderdetails.order_LN_MIN_TEMP,
                        priority: unplannedorderdetails.order_PRTY_TYPE,
                        revenueMiles: unplannedorderdetails.rvnu_MI,
                        revenueQuantity: unplannedorderdetails.rvnu_QTY,
                        updateDate: unplannedorderdetails.updateDate,
                        updateUserId: unplannedorderdetails.updateUserId
                    }
                    t.rollback();
                    esDao.esUpdateOrderDetails(existingParams).then(function (result) {
                        logger.info(util.format("%s::%s: Info:%s", LOG_CATEGORY, logMethod, util.inspect(" Es Rollback completed successfully")));
                        errMsg = "Update of order details failed..";
                        deferred.reject(errMsg);
                    }).catch(function (err) {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject(errMsg);
                    });
                });
            }).then((res) => {
                deferred.resolve("Order details updated successfully.");
            }).catch(function (err) {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(errMsg);
            });
        }).catch(function (err) {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject(errMsg);
        });
    }).catch((err) => {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        deferred.reject(errMsg);
    });

    return deferred.promise;
}

let _dbUpdateOrderDetails = function (params, dbConnection, transObj) {
    let logMethod = "_dbUpdateOrderDetails";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        var inputParm = JSON.stringify(params)
        dbConnection.query('Select  fn_Update_Order_Details (?);', {
            replacements: [inputParm],
            type: dbConnection.QueryTypes.SELECT
        }, transObj).then(function (res) {
            if (res && res.length > 0) {
                logger.info("order details updated in mySql DB  successfully.");
                let p = Object.values(res[0]);
                deferred.resolve(JSON.parse(p));
            } else {
                logger.info("No records updated .");
                deferred.resolve("No records updated .")
            }

        }).catch(err => {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject(err);
        });
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw new error(err);
    }

    return deferred.promise;
}

let updateOrderSequence = function (body) {
    let logMethod = "updateOrderSequence";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        sequlizeConn.getMySQLConn().then((dbConnection) => {
            dbConnection.transaction(function (t) {
                let transObj = {
                    transaction: t
                };
                return esDao.esUpdateOrderSeq(body, transObj).then((result) => {
                    return _dbUpdateOrderSeq(body, dbConnection, transObj).then((res) => {
                        t.commit();
                        deferred.resolve("order sequence updated successfully..");
                    }).catch((err) => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        t.rollback();
                        deferred.reject(err);
                    });
                }).catch((err) => {
                    logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                    t.rollback();
                    deferred.reject(err);
                });


            }).then((res) => {
                logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                deferred.resolve("Order Sequance details updated successfully.");
            }).catch(function (err) {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(err);
            });
        }).catch((err) => {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject(err);
        });

    } catch (error) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw new error(err);
    }
    return deferred.promise;
}

let _dbUpdateOrderSeq = function (body, dbConnection, transObj) {
    let logMethod = "_dbUpdateOrderSeq";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    try {
        var inputParm = JSON.stringify(body);
        dbConnection
            .query('Select  fn_Update_Order_Seq (?);', {
                replacements: [inputParm],
                type: dbConnection.QueryTypes.SELECT
            }, transObj).then(function (res) {
                if (res && res.length > 0) {
                    logger.info("order Sequence details updated  successfully.");
                    let p = Object.values(res[0]);
                    deferred.resolve(p);
                }
            }).catch(err => {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(err);
            });

    } catch (error) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw new error(err);
    }
    return deferred.promise;
}


let getOrderById = async (id) => {
    let logMethod = "getOrderById";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    try {
        const response = await client.search({
            index: '' + cfg.indxName.unplannedOrdersINDX + '',
            type: 'doc',
            body: {
                query: {
                    match: {
                        order_ID: id
                    }
                }
            }
        });
        return {
            "order": _.map(response.hits.hits, '_source')
        };
    } catch (err) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw err;
    }
}
/**
 * This function is to append order or set of orders to manifest
 * @param {object} orderlineId  (In Array) 
 * @param {string} manifestSourceId
 */
var AppendSelectedOrders = (orderlineId, manifestSourceId) => {
    var logMethod = 'AppendSelectedOrders';
    var deferred = Q.defer();

    try {
        var promises = [
            validation.isRequired("orderlineId", orderlineId),
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    sequlizeConn.getMySQLConn().then((conn) => {
                        return conn.transaction().then(function (t) {
                            return orderlineSchema(conn).update({
                                manifestSourceId: manifestSourceId,
                                orderlineStatus: cfg.statusP
                            }, {
                                where: {
                                    orderlineId: {
                                        [Op.in]: orderlineId
                                    }
                                }
                            }, {
                                transaction: t
                            }).then(function (res) {
                                return client.updateByQuery({
                                    index: '' + cfg.indxName.unplannedOrdersINDX + '',
                                    type: 'doc',
                                    body: {
                                        "query": {
                                            "bool": {
                                                "filter": {
                                                    "terms": {
                                                        "order_LN_ID": orderlineId
                                                    }
                                                }
                                            }
                                        },
                                        "script": {
                                            "inline": "ctx._source.order_STTS = '" + cfg.statusP + "';  ctx._source.mnfst_SRC_ID = '" + manifestSourceId + "'"
                                        }
                                    }
                                });
                            }).then(function (res) {
                                _checkBusinessRule(manifestSourceId);
                                return "Checking Business Rule";
                            }).then((res) => {
                                t.commit();
                                deferred.resolve({
                                    status: "success",
                                    message: "Appended Successfully"
                                });

                            }).catch((err) => {
                                t.rollback();
                                deferred.reject(err);
                            });
                        });
                    });


                } else {
                    logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                    logger.error(msg);
                    deferred.reject(msg);
                }
            });
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/**
 * This function is to check business rules
 * @param {number} businessRuleId
 * @param {string} manifestSourceId
 */
var CheckBusinessRules = function (businessRuleId, manifestSourceId) {
    var logMethod = "CheckBusinessRules";
    var deferred = Q.defer();
    var dataJson = {};
    try {
        var promises = [
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    sequlizeConn.selectData("SELECT * FROM `configurations`", dataJson).then(function (res) {
                        if (businessRuleId === cfg.businessRules.BR_PLN_1) {
                            return _checkRuleBR_PLN_1(manifestSourceId, res[0]);
                        } else if (businessRuleId === cfg.businessRules.BR_PLN_2) {
                            return _checkRuleBR_PLN_2(manifestSourceId, res[0]);
                        } else if (businessRuleId === cfg.businessRules.BR_PLN_3) {
                            return _checkRuleBR_PLN_3(manifestSourceId, res[0]);
                        } else if (businessRuleId === cfg.businessRules.BR_PLN_6) {
                            return _checkRuleBR_PLN_6(manifestSourceId);
                        } else if (businessRuleId === cfg.businessRules.BR_PLN_7) {
                            return _checkRuleBR_PLN_7(manifestSourceId);
                        } else {
                            return "No functions";
                        }
                    }).then(function (res) {
                        deferred.resolve(res);
                    }).catch(err => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject(err);
                    });
                } else {
                    logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                    logger.error(msg);
                    deferred.reject(msg);
                }
            });
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/**
 * CheckAppointmentWindow
 * @param {number} orderlineId
 */
var CheckAppointmentWindow = function (orderlineId) {
    var logMethod = "CheckAppointmentWindow";
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("orderlineId", orderlineId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    client.search({
                        index: '' + cfg.indxName.unplannedOrdersINDX + '',
                        type: 'doc',
                        body: {
                            query: {
                                match: {
                                    order_LN_ID: orderlineId
                                }
                            },
                            _source: ["frght_TYPE", "order_LN_DLVRY_STRT_TM", "order_LN_PCKUP_END_TM", "order_LN_DLVRY_END_TM", "order_LN_PCKUP_STRT_TM"]
                        }
                    }).then((res) => {
                        if (res.hits.hits.length > 0) {
                            var orderDetails = res.hits.hits[0]._source;
                            if ((orderDetails.frght_TYPE === cfg.frightTypes.inbound &&
                                    ([null, undefined, ''].includes(orderDetails.order_LN_PCKUP_STRT_TM)) &&
                                    ([null, undefined, ''].includes(orderDetails.order_LN_PCKUP_END_TM))) ||
                                (orderDetails.frght_TYPE === cfg.frightTypes.outbound &&
                                    ([null, undefined, ''].includes(orderDetails.order_LN_DLVRY_STRT_TM)) &&
                                    ([null, undefined, ''].includes(orderDetails.order_LN_DLVRY_END_TM)))) {
                                deferred.reject({
                                    "status": cfg.businessRuleStatus.WARNING
                                });
                            } else {
                                deferred.resolve({
                                    "status": "success"
                                });
                            }
                        } else {
                            deferred.reject("No orders");
                        }

                    }).catch((err) => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject(err);
                    });
                } else {
                    logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                    logger.error(msg);
                    deferred.reject(msg);
                }
            });
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/**
 * Calculate weight for each manifest 
 * @param {string} manifestSourceId 
 */
var getCommodityWeight = (manifestSourceId) => {
    var logMethod = 'getCommodityWeight';
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    const response = client.search({
                        index: '' + cfg.indxName.unplannedOrdersINDX + '',
                        type: 'doc',
                        body: {
                            query: {
                                match: {
                                    mnfst_SRC_ID: manifestSourceId
                                }
                            },
                            aggs: {
                                result: {
                                    sum: {
                                        script: {
                                            source: "Double.parseDouble(doc['weight.keyword'].value)"
                                        }
                                    }
                                }
                            }
                        }
                    });
                    response.then(resp => {
                        deferred.resolve({
                            "status": "success",
                            "commodityWeight": resp.aggregations.result.value
                        })
                    }).catch(err => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject(err);
                    })
                }
            })
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/**
 * To get Pallet Count for planned Order 
 * @param {string} manifestSourceId 
 */
var getLaneDensityPalletcount = (manifestSourceId) => {
    var logMethod = 'getLaneDensityPalletcount';
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    const response = client.search({
                        index: '' + cfg.indxName.unplannedOrdersINDX + '',
                        type: 'doc',
                        body: {
                            "query": {
                                "bool": {
                                    "must": [{
                                        "term": {
                                            "mnfst_SRC_ID.keyword": manifestSourceId
                                        }
                                    }]
                                }
                            },
                            "aggs": {
                                "palletCount": {
                                    "sum": {
                                        "field": "order_LN_PLET_CNT"
                                    }
                                }
                            }
                        }
                    });
                    response.then(resp => {
                        deferred.resolve({
                            "status": "success",
                            "palletCount": resp.aggregations.palletCount.value
                        })
                    }).catch(err => {
                        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                        deferred.reject(err);
                    })
                }
            })
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/**
 * This function is to remove orders from manifest
 * @param {string} manifestSourceId 
 */
var removeOrderFromManifest = function (orderlineId, manifestSourceId) {
    var logMethod = "removeOrderFromManifest";
    var deferred = Q.defer();
    try {
        var promises = [
            validation.isRequired("orderlineId", orderlineId),
            validation.isRequired("manifestSourceId", manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    }
                }
                if (msg.length == 0) {
                    sequlizeConn.getMySQLConn().then((conn) => {
                        return conn.transaction().then(function (t) {
                            return orderlineSchema(conn).update({
                                manifestSourceId: null,
                                orderlineStatus: cfg.statusA
                            }, {
                                where: {
                                    orderlineId: {
                                        [Op.in]: orderlineId
                                    }
                                }
                            }, {
                                transaction: t
                            }).then(function (res) {
                                return client.updateByQuery({
                                    index: '' + cfg.indxName.unplannedOrdersINDX + '',
                                    type: 'doc',
                                    body: {
                                        "query": {
                                            "bool": {
                                                "filter": {
                                                    "terms": {
                                                        "order_LN_ID": orderlineId
                                                    }
                                                }
                                            }
                                        },
                                        "script": {
                                            "inline": "ctx._source.order_STTS = '" + cfg.statusA + "';  ctx._source.mnfst_SRC_ID = null"
                                        }
                                    }
                                });
                            }).then(function (res) {
                                _checkBusinessRule(manifestSourceId);
                                return "Checking Business Rules";
                            }).then((res) => {
                                t.commit();
                                deferred.resolve({
                                    status: "success",
                                    message: "Removed Successfully"
                                });

                            }).catch((err) => {
                                t.rollback();
                                deferred.reject(err);
                            });
                        });
                    }).catch((err) => {
                        t.rollback();
                        deferred.reject(err);
                    });
                } else {
                    logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
                    logger.error(msg);
                    deferred.reject(msg);
                }
            });
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/**
 * Internal function - To Check Business rules
 */
var _checkBusinessRule = function (manifestSourceId) {
    var logMethod = "_checkBusinessRule";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    var dataJson = {};
    sequlizeConn.selectData("SELECT * FROM `configurations`", dataJson).then(function (res) {
        var promises = [
            _checkRuleBR_PLN_1(manifestSourceId, res[0]),
            _checkRuleBR_PLN_2(manifestSourceId, res[0]),
            _checkRuleBR_PLN_3(manifestSourceId, res[0]),
            _checkRuleBR_PLN_6(manifestSourceId),
            _checkRuleBR_PLN_7(manifestSourceId)
        ];
        Q.allSettled(promises)
            .then(function (results) {
                var msg = [];
                var manifestStatusArray = [];
                for (var i = 0; i < results.length; i++) {
                    if (results[i].state == 'rejected') {
                        msg.push(results[i].reason);
                    } else {
                        manifestStatusArray.push(results[i].value.status);
                    }
                }
                if (msg.length == 0) {
                    var manifestStatus = cfg.businessRuleStatus.ALLGOOD;
                    if ((manifestStatusArray.includes(cfg.businessRuleStatus.CRITICAL))) {
                        manifestStatus = cfg.businessRuleStatus.CRITICAL;
                    } else if ((manifestStatusArray.includes(cfg.businessRuleStatus.WARNING))) {
                        manifestStatus = cfg.businessRuleStatus.WARNING
                    }
                    sequlizeConn.getMySQLConn().then((conn) => {
                        return conn.transaction().then(function (t) {
                            return manifestSchema(conn).findAll({
                                where: {
                                    manifestSrcId: {
                                        [Op.eq]: manifestSourceId
                                    }
                                }
                            }, {
                                transaction: t
                            }).then(function (res) {
                                return manifestSchema(conn).update({
                                    manifestStatus: manifestStatus
                                }, {
                                    where: {
                                        manifestId: {
                                            [Op.eq]: res[0].manifestId
                                        }
                                    }
                                }, {
                                    transaction: t
                                })
                            }).then(function (res) {
                                return client.updateByQuery({
                                    index: '' + cfg.indxName.manifestINDX + '',
                                    type: 'doc',
                                    body: {
                                        "query": {
                                            "match": {
                                                "sourceId": manifestSourceId
                                            }
                                        },
                                        "script": {
                                            "inline": "ctx._source.status = '" + manifestStatus + "'"
                                        }
                                    }
                                });
                            }).then((res) => {
                                t.commit();
                                deferred.resolve({
                                    "manifestStatus": manifestStatus
                                });
                            }).catch((err) => {
                                t.rollback();
                                deferred.reject(err);
                            });
                        });
                    }).catch((err) => {
                        t.rollback();
                        deferred.reject(err);
                    });

                } else {
                    logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(msg)));
                    deferred.reject(msg);
                }
            }).catch(err => {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(err);
            });
    }).catch(err => {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        deferred.reject(err);
    });
    return deferred.promise;
}
/**
 * Internal function -  To Check Business rule 1: BR_PLN_1
 */
var _checkRuleBR_PLN_1 = function (manifestSourceId, configData) {
    var logMethod = "_checkRuleBR_PLN_1";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    client.search({
        index: '' + cfg.indxName.unplannedOrdersINDX + '',
        type: 'doc',
        body: {
            query: {
                match: {
                    mnfst_SRC_ID: manifestSourceId
                }
            },
            aggs: {
                totalPltCount: {
                    sum: {
                        field: "order_LN_PLET_CNT"
                    }
                }
            }
        }
    }).then(function (res) {
        var totalPltCount = res.aggregations.totalPltCount.value
        if (totalPltCount > configData.CONFIG_MXMM_PLLT_CPCTY) {
            client.search({
                index: '' + cfg.indxName.manifestINDX + '',
                type: 'doc',
                body: {
                    _source: ["id"],
                    query: {
                        match: {
                            sourceId: manifestSourceId
                        }
                    }
                }
            }).then(resp => {
                var warningErrorMag = "Maximum Pallet Count Allowed is " + configData.CONFIG_MXMM_PLLT_CPCTY;
                return _updateManifestErrorLog(resp.hits.hits[0]._source.id, cfg.businessRules.BR_PLN_1, warningErrorMag)
            }).then(resp => {
                deferred.resolve({
                    "status": cfg.businessRuleStatus.CRITICAL
                });
            }).catch(err => {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(err);
            });
        } else {
            deferred.resolve({
                "status": cfg.businessRuleStatus.ALLGOOD
            });
        }
    }).catch(err => {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        deferred.reject(err);
    });


    return deferred.promise;
}
/**
 * Internal function -  To Check Business rule 2: BR_PLN_2
 */
var _checkRuleBR_PLN_2 = function (manifestSourceId, configData) {
    var logMethod = "_checkRuleBR_PLN_2";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    client.search({
        index: '' + cfg.indxName.unplannedOrdersINDX + '',
        type: 'doc',
        body: {
            query: {
                match: {
                    mnfst_SRC_ID: manifestSourceId
                }
            },
            aggs: {
                totalWeight: {
                    sum: {
                        script: {
                            source: "Double.parseDouble(doc['weight.keyword'].value)"
                        }
                    }
                }
            }
        }
    }).then(function (res) {
        var totalWeight = res.aggregations.totalWeight.value
        //CONFIG_TLRBL_LMT_WGHT_CPCTY => percentage
        var maxWeight = parseInt(configData.CONFIG_MXMM_WGHT_CPCTY);
        var maxWeightWithTP = maxWeight * (1 + (parseInt(configData.CONFIG_TLRBL_LMT_WGHT_CPCTY) / 100)); //W*(1 + TP/100) 
        if (totalWeight > maxWeight && maxWeightWithTP > totalWeight) {
            client.search({
                index: '' + cfg.indxName.manifestINDX + '',
                type: 'doc',
                body: {
                    _source: ["id"],
                    query: {
                        match: {
                            sourceId: manifestSourceId
                        }
                    }
                }
            }).then(resp => {
                var warningErrorMag = "Loaded weight has exceeded truck capacity of " + maxWeight + " lbs";
                return _updateManifestErrorLog(resp.hits.hits[0]._source.id, cfg.businessRules.BR_PLN_2, warningErrorMag)
            }).then(resp => {
                deferred.resolve({
                    "status": cfg.businessRuleStatus.WARNING
                });
            }).catch(err => {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(err);
            });
        } else {
            deferred.resolve({
                "status": cfg.businessRuleStatus.ALLGOOD
            });
        }
    }).catch(err => {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        deferred.reject(err);
    });
    return deferred.promise;
}
/**
 * Internal function -  To Check Business rule 3: BR_PLN_3
 */
var _checkRuleBR_PLN_3 = function (manifestSourceId, configData) {
    var logMethod = "_checkRuleBR_PLN_3";
    logger.info(util.format("%s::%s->", LOG_CATEGORY, logMethod));
    var deferred = Q.defer();
    client.search({
        index: '' + cfg.indxName.unplannedOrdersINDX + '',
        type: 'doc',
        body: {
            query: {
                match: {
                    mnfst_SRC_ID: manifestSourceId
                }
            },
            aggs: {
                totalWeight: {
                    sum: {
                        script: {
                            source: "Double.parseDouble(doc['weight.keyword'].value)"
                        }
                    }
                }
            }
        }
    }).then(function (res) {
        var totalWeight = res.aggregations.totalWeight.value
        var maxWeight = parseInt(configData.CONFIG_MXMM_WGHT_CPCTY);
        var maxWeightWithTP = maxWeight * (1 + (parseInt(configData.CONFIG_TLRBL_LMT_WGHT_CPCTY) / 100));
        if (maxWeightWithTP < totalWeight) {
            client.search({
                index: '' + cfg.indxName.manifestINDX + '',
                type: 'doc',
                body: {
                    _source: ["id"],
                    query: {
                        match: {
                            sourceId: manifestSourceId
                        }
                    }
                }
            }).then(resp => {
                var warningErrorMag = "Loaded weight has exceeded truck capacity " + maxWeight + " lbs by more than " + parseInt(configData.CONFIG_TLRBL_LMT_WGHT_CPCTY) + "%";
                return _updateManifestErrorLog(resp.hits.hits[0]._source.id, cfg.businessRules.BR_PLN_3, warningErrorMag)
            }).then(resp => {
                deferred.resolve({
                    "status": cfg.businessRuleStatus.WARNING
                })
            }).catch(err => {
                logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                deferred.reject(err);
            });
        } else {
            deferred.resolve({
                "status": cfg.businessRuleStatus.ALLGOOD
            });
        }
    }).catch(err => {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        deferred.reject(err);
    });


    return deferred.promise;
}

/**
 * Internal function - To check Load type for BR_PLN_6
 */
var _checkRuleBR_PLN_6 = function (manifestSourceId) {
    var logMethod = '_checkRuleBR_PLN_6';
    var deferred = Q.defer();
    try {
        client.search({
            index: '' + cfg.indxName.unplannedOrdersINDX + '',
            type: 'doc',
            body: {
                _source: ["mnfst_SRC_ID", "order_LN_LOAD_TYPE", "order_LN_ID"],
                query: {
                    match: {
                        mnfst_SRC_ID: manifestSourceId
                    }
                }
            }
        }).then(result => {
            if (result.hits.hits.length) {
                var loadData = result.hits.hits;
                client.search({
                    index: '' + cfg.indxName.manifestINDX + '',
                    type: 'doc',
                    body: {
                        _source: ["sourceId", "loadType", "id"],
                        query: {
                            match: {
                                sourceId: manifestSourceId
                            }
                        }
                    }
                }).then(manifestResult => {
                    if (manifestResult.hits.hits.length) {
                        if (Object.keys(manifestResult.hits.hits[0]._source).length >= 0 && !([undefined].includes(manifestResult.hits.hits[0]._source.loadType))) {
                            var loadType = manifestResult.hits.hits[0]._source.loadType;
                            var status = false;
                            loadData.map(loadResp => {
                                if (loadResp._source.order_LN_LOAD_TYPE != loadType) {
                                    status = true;
                                }
                            })
                            if (!status) {
                                deferred.resolve({
                                    "status": cfg.businessRuleStatus.ALLGOOD
                                })
                            } else {
                                var warningErrorMag = "";
                                loadData.map(loadResp => {
                                    warningErrorMag += "Manifest '" + manifestSourceId + "' is an '" + loadType + "' whereas Order '" + loadResp._source.order_LN_ID + "' is an '" + loadResp._source.order_LN_LOAD_TYPE + "' order" + ",";
                                });
                                _updateManifestErrorLog(manifestResult.hits.hits[0]._source.id, cfg.businessRules.BR_PLN_6, warningErrorMag).then(resp => {
                                    deferred.resolve({
                                        "status": cfg.businessRuleStatus.WARNING
                                    })
                                })
                            }
                        }
                    }
                }).catch(err => {
                    logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                    deferred.reject(err);
                });
            }
        }).catch(err => {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject("There is no manifest");
        });
    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/*
 * Internal function -  To checkFreightType for BR_PLN_7
 */
var _checkRuleBR_PLN_7 = function (manifestSourceId) {
    let logMethod = '_checkRuleBR_PLN_7';
    var deferred = Q.defer();
    try {
        client.search({
            index: '' + cfg.indxName.unplannedOrdersINDX + '',
            type: 'doc',
            body: {
                _source: ["mnfst_SRC_ID", "frght_TYPE", "order_LN_ID"],
                query: {
                    match: {
                        mnfst_SRC_ID: manifestSourceId
                    }
                }
            }
        }).then(result => {
            if (result.hits.hits.length) {
                var unplannedOrderData = result.hits.hits;
                client.search({
                    index: '' + cfg.indxName.manifestINDX + '',
                    type: 'doc',
                    body: {
                        _source: ["sourceId", "freightType", "id"],
                        query: {
                            match: {
                                sourceId: manifestSourceId
                            }
                        }
                    }
                }).then(manifestResult => {
                    if (manifestResult.hits.hits.length) {
                        if (Object.keys(manifestResult.hits.hits[0]._source).length > 0 && !([null, undefined, ''].includes(manifestResult.hits.hits[0]._source.freightType))) {
                            var freightType = manifestResult.hits.hits[0]._source.freightType;
                            var status = false;
                            unplannedOrderData.map(frghtResp => {
                                if (frghtResp._source.frght_TYPE.toLowerCase().trim() != freightType.toLowerCase().trim()) {
                                    status = true;
                                }
                            })
                            if (!status) {
                                deferred.resolve({
                                    "status": cfg.businessRuleStatus.ALLGOOD
                                })
                            } else {
                                var criticalErrorMag = "";
                                unplannedOrderData.map(frghtResp => {
                                    criticalErrorMag += "Manifest '" + manifestSourceId + "' is an '" + freightType + "' whereas Order '" + frghtResp._source.order_LN_ID + "' is an '" + frghtResp._source.frght_TYPE + "' order" + ",";
                                });
                                _updateManifestErrorLog(manifestResult.hits.hits[0]._source.id, cfg.businessRules.BR_PLN_7, criticalErrorMag).then(resp => {
                                    deferred.resolve({
                                        "status": cfg.businessRuleStatus.CRITICAL
                                    })
                                })
                            }
                        } else {
                            /////////// For First time, Order move to manifest /////////////////
                            ////////// Mysql Update for freightType ////////////////
                            sequlizeConn.getMySQLConn().then((conn) => {
                                return conn.transaction().then(function (t) {
                                    return manifestSchema(conn).update({
                                        manifestFrightType: unplannedOrderData[0]._source.frght_TYPE
                                    }, {
                                        where: {
                                            manifestSrcId: {
                                                [Op.eq]: manifestSourceId
                                            }
                                        }
                                    }, {
                                        transaction: t
                                    }).then(function (res) {
                                        ////////// Index Update for freightType ////////////////
                                        return client.updateByQuery({
                                            index: '' + cfg.indxName.manifestINDX + '',
                                            type: 'doc',
                                            body: {
                                                "query": {
                                                    "match": {
                                                        "sourceId": manifestSourceId
                                                    }
                                                },
                                                "script": {
                                                    "inline": "ctx._source.freightType = '" + unplannedOrderData[0]._source.frght_TYPE + "'"
                                                }
                                            }
                                        });
                                    }).then(function (res) {
                                        var status = false;
                                        unplannedOrderData.map(frghtResp => {
                                            if (frghtResp._source.frght_TYPE.toLowerCase().trim() != unplannedOrderData[0]._source.frght_TYPE.toLowerCase().trim()) {
                                                status = true;
                                            }
                                        })
                                        if (!status) {
                                            deferred.resolve({
                                                "status": "matched"
                                            })
                                        } else {
                                            var criticalErrorMag = "";
                                            unplannedOrderData.map(frghtResp => {
                                                criticalErrorMag += "Manifest '" + manifestSourceId + "' is an '" + freightType + "' whereas Order '" + frghtResp._source.order_LN_ID + "' is an '" + frghtResp._source.frght_TYPE + "' order" + ",";
                                            });
                                            _updateManifestErrorLog(manifestResult.hits.hits[0]._source.id, cfg.businessRules.BR_PLN_7, criticalErrorMag).then(resp => {
                                                deferred.resolve({
                                                    "status": cfg.businessRuleStatus.CRITICAL
                                                })
                                            })
                                        }
                                        t.commit();
                                    }).catch((err) => {
                                        t.rollback();
                                        deferred.reject(err);
                                    });
                                });
                            });
                        }
                    }
                }).catch(err => {
                    logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
                    deferred.reject(err);
                });
            }
        }).catch(err => {
            logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
            deferred.reject("There is no manifest");
        });

    } catch (err) {
        logger.info(util.format("<-%s::%s", LOG_CATEGORY, logMethod));
        logger.error(err);
        throw err;
    }
    return deferred.promise;
}
/*
 * Internal function -  To update manifest error log
 */
var _updateManifestErrorLog = function (manifestId, manifestRuleId, manifestRuleMsg) {
    var logMethod = "_updateManifestErrorLog";
    var deferred = Q.defer();
    try {
        sequlizeConn.getMySQLConn().then((conn) => {
            return conn.transaction().then(function (t) {
                return manifestErrorLogSchema(conn).destroy({
                    where: {
                        [Op.and]: [{
                            manifestSrcId: manifestId
                        }, {
                            manifestRuleId: manifestRuleId
                        }]
                    }
                }, {
                    transaction: t
                }).then(function (res) {
                    return manifestErrorLogSchema(conn).create({
                        manifestSrcId: manifestId,
                        manifestRuleId: manifestRuleId,
                        manifestRuleMsg: manifestRuleMsg
                    }, {
                        transaction: t
                    }).then(function (res) {
                        t.commit();
                        deferred.resolve({
                            "status": "Error log Updated"
                        })
                    }).catch(err => {
                        t.rollback();
                        deferred.reject(err);
                    })
                })
            })
        })

    } catch (error) {
        logger.error(util.format("%s::%s: err:%s", LOG_CATEGORY, logMethod, util.inspect(err)));
        throw new error(err);
    }
    return deferred.promise;
}

// Exporting modules
module.exports = {
    getUnplannedOrdersList: getUnplannedOrdersList,
    updateOrderDetails: updateOrderDetails,
    updateOrderSequence: updateOrderSequence,
    getOrderById: getOrderById,
    AppendSelectedOrders: AppendSelectedOrders,
    CheckBusinessRules: CheckBusinessRules,
    CheckAppointmentWindow: CheckAppointmentWindow,
    getCommodityWeight: getCommodityWeight,
    getLaneDensityPalletcount: getLaneDensityPalletcount,
    removeOrderFromManifest: removeOrderFromManifest
}