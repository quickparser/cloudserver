const { errors, versioning } = require('arsenal');
const async = require('async');

const metadata = require('../../../metadata/wrapper');
const { config } = require('../../../Config');
const { addIsNonversionedBucket } = require('../../../metadata/metadataUtils');

const versionIdUtils = versioning.VersionID;
// Use Arsenal function to generate a version ID used internally by metadata
// for null versions that are created before bucket versioning is configured
const nonVersionedObjId =
    versionIdUtils.getInfVid(config.replicationGroupId);

/** decodedVidResult - decode the version id from a query object
 * @param {object} [reqQuery] - request query object
 * @param {string} [reqQuery.versionId] - version ID sent in request query
 * @return {(Error|string|undefined)} - return Invalid Argument if decryption
 * fails due to improper format, otherwise undefined or the decoded version id
 */
function decodeVersionId(reqQuery) {
    if (!reqQuery || !reqQuery.versionId) {
        return undefined;
    }
    let versionId = reqQuery.versionId;
    if (versionId === 'null') {
        return versionId;
    }
    versionId = versionIdUtils.decode(versionId);
    if (versionId instanceof Error) {
        return errors.InvalidArgument
            .customizeDescription('Invalid version id specified');
    }
    return versionId;
}

/** getVersionIdResHeader - return encrypted version ID if appropriate
 * @param {object} [verCfg] - bucket versioning configuration
 * @param {object} objectMD - object metadata
 * @return {(string|undefined)} - undefined or encrypted version ID
 * (if not 'null')
 */
function getVersionIdResHeader(verCfg, objectMD) {
    if (verCfg) {
        if (objectMD.isNullKey || objectMD.isNull || !objectMD.versionId) {
            return 'null';
        }
        return versionIdUtils.encode(objectMD.versionId,
                                     config.versionIdEncodingType);
    }
    return undefined;
}

/**
 * Checks for versionId in request query and returns error if it is there
 * @param {object} query - request query
 * @return {(Error|undefined)} - customized InvalidArgument error or undefined
 */
function checkQueryVersionId(query) {
    if (query && query.versionId !== undefined) {
        const customMsg = 'This operation does not accept a version-id.';
        return errors.InvalidArgument.customizeDescription(customMsg);
    }
    return undefined;
}

function _storeNullVersionMD(bucketName, objKey, objMD, log, cb) {
    metadata.putObjectMD(bucketName, objKey, objMD, { versionId: 'null' }, log, err => {
        if (err) {
            log.debug('error from metadata storing null version as new version',
            { error: err });
        }
        cb(err);
    });
}

/** get location of null version data for deletion
* @param {string} bucketName - name of bucket
* @param {string} objKey - name of object key
* @param {object} options - metadata options for getting object MD
* @param {string} options.versionId - version to get from metadata
* @param {object} mst - info about the master version
* @param {string} mst.versionId - the master version's version id
* @param {RequestLogger} log - logger instanceof
* @param {function} cb - callback
* @return {undefined} - and call callback with (err, dataToDelete)
*/
function _getNullVersionLocationsToDelete(bucketName, objKey, options, mst, log, cb) {
    if (options.versionId === mst.versionId) {
        // no need to get location, we already have the master's metadata
        const dataToDelete = mst.objLocation;
        return process.nextTick(cb, null, dataToDelete);
    }
    return metadata.getObjectMD(bucketName, objKey, options, log,
        (err, versionMD) => {
            if (err) {
                if (!err.is.NoSuchKey) {
                    log.warn('could not get null version metadata', {
                        error: err,
                        method: '_getNullVersionLocationsToDelete',
                    });
                    return cb(err);
                }
                return cb();
            }
            if (!versionMD.location) {
                return cb();
            }
            const dataToDelete = Array.isArray(versionMD.location) ?
                versionMD.location : [versionMD.location];
            return cb(null, dataToDelete);
        });
}

function _deleteNullVersionMD(bucketName, objKey, options, log, cb) {
    return metadata.deleteObjectMD(bucketName, objKey, options, log, err => {
        if (err) {
            log.warn('metadata error deleting null version',
                     { error: err, method: '_deleteNullVersionMD' });
            return cb(err);
        }
        return cb();
    });
}

/**
 * Process state from the master version of an object and the bucket
 * versioning configuration, return a set of options objects
 *
 * @param {object} mst - state of master version, as returned by
 * getMasterState()
 * @param {string} vstat - bucket versioning status: 'Enabled' or 'Suspended'
 *
 * @return {object} result object with the following attributes:
 * - {object} options: versioning-related options to pass to the
     services.metadataStoreObject() call
 * - {object} [storeOptions]: options for metadata to create a new
     null version key, if needed
 * - {object} [delOptions]: options for metadata to delete the null
     version key, if needed
 */
function processVersioningState(mst, vstat) {
    const options = {};
    const storeOptions = {};
    const delOptions = {};
    // object does not exist, or is not versioned (before versioning),
    // or is a current null version (old null versioned key style with
    // 'isNull' or new null key style with 'isNullKey')
    if (mst.isNullKey || mst.isNull || !mst.versionId) {
        // versioning is suspended, overwrite existing master version
        if (vstat === 'Suspended') {
            options.versionId = '';
            options.isNull = true;
            options.dataToDelete = mst.objLocation;
            // TODO pass 'oldReplayId' to PUT request

            // if a null versioned key exists, clean it up prior to put
            //
            // NOTE: this is for backward-compatibility with older
            // null versions that are stored as versioned keys
            if (mst.isNull) {
                delOptions.versionId = mst.versionId;
                return { options, delOptions };
            }
            // clean up the null key prior to put
            if (mst.isNullKey) {
                delOptions.versionId = 'null';
                return { options, delOptions };
            }
            return { options };
        }
        // versioning is enabled, create a new version
        options.versioning = true;
        if (mst.exists) {
            // store master version in the null key
            storeOptions.versionId = mst.isNull || mst.isNullKey ? mst.versionId : nonVersionedObjId;
            storeOptions.isNullKey = true;
            return { options, storeOptions };
        }
        return { options };
    }
    // master is versioned and is not a null version
    if (vstat === 'Suspended') {
        // versioning is suspended, overwrite the existing master version
        // TODO pass 'oldReplayId' to PUT request
        options.versionId = '';
        options.isNull = true;
    } else {
        // versioning is enabled, put the new version
        options.versioning = true;
    }

    // deal with legacy nullVersionId
    //
    // NOTE: this is for backward-compatibility with older
    // null versions that are stored as versioned keys
    const nullVersionId = mst.nullVersionId;
    if (nullVersionId) {
        if (vstat === 'Suspended') {
            // delete the null versioned object
            delOptions.versionId = nullVersionId;
            delOptions.deleteData = true;
            if (mst.nullUploadId) {
                delOptions.replayId = mst.nullUploadId;
            }
            return { options, delOptions };
        }

        // transform the null versioned key into a null key
        storeOptions.versionId = nullVersionId;
        storeOptions.isNullKey = true;
        delOptions.versionId = nullVersionId;
        return { options, storeOptions, delOptions };
    }
    if (vstat === 'Suspended') {
        // need to get the null key to delete it, it may or not exist
        delOptions.versionId = 'null';
        delOptions.deleteData = true;
        return { options, delOptions };
    }
    return { options };
}

/**
 * Build the state of the master version from its object metadata
 *
 * @param {object} objMD - object metadata parsed from JSON
 *
 * @return {object} state of master version, with the following attributes:
 * - {boolean} exists - true if the object exists (i.e. if `objMD` is truish)
 * - {string} versionId - version ID of the master key
 * - {boolean} isNull - whether the master version is a null version
 * - {string} nullVersionId - if not a null version, reference to the
 *   null version ID
 * - {array} objLocation - array of data locations
 */
function getMasterState(objMD) {
    if (!objMD) {
        return {};
    }
    const mst = {
        exists: true,
        versionId: objMD.versionId,
        uploadId: objMD.uploadId,
        isNull: objMD.isNull,
        isNullKey: objMD.isNullKey,
        nullVersionId: objMD.nullVersionId,
        nullUploadId: objMD.nullUploadId,
    };
    if (objMD.location) {
        mst.objLocation = Array.isArray(objMD.location) ?
            objMD.location : [objMD.location];
    }
    return mst;
}
/** versioningPreprocessing - return versioning information for S3 to handle
 * creation of new versions and manage deletion of old data and metadata
 * @param {string} bucketName - name of bucket
 * @param {object} bucketMD - bucket metadata
 * @param {string} objectKey - name of object
 * @param {object} objMD - obj metadata
 * @param {RequestLogger} log - logger instance
 * @param {function} callback - callback
 * @return {undefined} and call callback with params (err, options):
 * options.dataToDelete - (array/undefined) location of data to delete
 * options.versionId - specific versionId to overwrite in metadata
 *  ('' overwrites the master version)
 * options.versioning - (true/undefined) metadata instruction to create new ver
 * options.isNull - (true/undefined) whether new version is null or not
 */
function versioningPreprocessing(bucketName, bucketMD, objectKey, objMD,
    log, callback) {
    const mst = getMasterState(objMD);
    const vCfg = bucketMD.getVersioningConfiguration();
    // bucket is not versioning configured
    if (!vCfg) {
        const options = { dataToDelete: mst.objLocation };
        return process.nextTick(callback, null, options);
    }
    // bucket is versioning configured
    const { options, storeOptions, delOptions } =
          processVersioningState(mst, vCfg.Status);
    return async.series([
        function storeVersion(next) {
            if (!storeOptions) {
                return process.nextTick(next);
            }
            const versionMD = Object.assign({}, objMD, storeOptions);
            return _storeNullVersionMD(bucketName, objectKey, versionMD, log, next);
        },
        function getNullVersionLocationsToDelete(next) {
            if (!delOptions || !delOptions.deleteData) {
                return process.nextTick(next);
            }
            return _getNullVersionLocationsToDelete(
                bucketName, objectKey, delOptions, mst, log,
                (err, nullDataToDelete) => {
                    if (err) {
                        return next(err);
                    }
                    if (nullDataToDelete) {
                        options.dataToDelete = nullDataToDelete;
                    }
                    return next();
                });
        },
        function deleteNullVersion(next) {
            if (!delOptions) {
                return process.nextTick(next);
            }
            return _deleteNullVersionMD(bucketName, objectKey, delOptions, log, err => {
                if (err) {
                    // it's possible there was a concurrent request to
                    // delete the null version, so proceed with putting a
                    // new version
                    if (err.is.NoSuchKey) {
                        return next(null, options);
                    }
                    return next(errors.InternalError);
                }
                return next();
            });
        },
    ], err => callback(err, options));
}

/** preprocessingVersioningDelete - return versioning information for S3 to
 * manage deletion of objects and versions, including creation of delete markers
 * @param {string} bucketName - name of bucket
 * @param {object} bucketMD - bucket metadata
 * @param {object} objectMD - obj metadata
 * @param {string} [reqVersionId] - specific version ID sent as part of request
 * @param {RequestLogger} log - logger instance
 * @param {function} callback - callback
 * @return {undefined} and call callback with params (err, options):
 * options.deleteData - (true/undefined) whether to delete data (if undefined
 *  means creating a delete marker instead)
 * options.versionId - specific versionId to delete
 * options.isNull - (true/undefined) whether version to be deleted/marked is null or not
 */
function preprocessingVersioningDelete(bucketName, bucketMD, objectMD,
    reqVersionId, log, callback) {
    const options = {};
    if (!bucketMD.getVersioningConfiguration() || reqVersionId) {
        // delete data if bucket is non-versioned or the request
        // deletes a specific version
        options.deleteData = true;
        if (objectMD.uploadId) {
            options.replayId = objectMD.uploadId;
        }
    }
    if (bucketMD.getVersioningConfiguration()) {
        if (reqVersionId) {
            if (reqVersionId === 'null') {
                // deleting the 'null' version if it exists
                if (objectMD.versionId !== undefined) {
                    if (objectMD.isNull) {
                        options.versionId = objectMD.versionId;
                    } else {
                        // deleting the null key
                        options.versionId = 'null';
                    }
                }
            } else {
                // deleting a specific version
                options.versionId = reqVersionId;
            }
        } else {
            // not deleting any specific version, making a delete marker instead
            options.isNull = true;
        }
    }
    return callback(null, options);
}

module.exports = {
    decodeVersionId,
    getVersionIdResHeader,
    checkQueryVersionId,
    processVersioningState,
    getMasterState,
    versioningPreprocessing,
    preprocessingVersioningDelete,
};
