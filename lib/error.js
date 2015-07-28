module.exports = function(err, requestParams) {
    Error.captureStackTrace(err, arguments.callee);
    if (requestParams) err.parameters = requestParams;
    return err;
}
