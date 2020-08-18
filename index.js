const { BlobSASPermissions, BlobServiceClient, StorageSharedKeyCredential, generateBlobSASQueryParameters: generateBlobSas } = require('@azure/storage-blob');
const { Readable } = require('stream');
const moment = require('moment');

module.exports.BlobStorageAdapter = class {
    /**
     * @param {string} account 
     * @param {string} key 
     */
    constructor(account, key) {
        this.url = `https://${account}.blob.core.windows.net/`;
        this.credential = new StorageSharedKeyCredential(account, key);
        this.blobService = new BlobServiceClient(this.url, this.credential);
    }

    /**
     * @param {string} container 
     * @param {string} filename 
     * @param {Number} validFor The number of time units this blob's shared access signature should be valid for
     * @param {string} validForUnit MomentJS compatible unit of time (e.g. minute, hour, day, etc.)
     */
    generateBlobSas(container, filename, validFor = 1, validForUnit = 'hour') {
        const start = moment().subtract(5, 'minute');
        const end = start.clone().add(validFor, validForUnit);

        const signature = {
            startsOn: start.toDate(),
            expiresOn: end.toDate(),
            permissions: BlobSASPermissions.parse('r'),
            containerName: container,
            blobName: filename
        }
        return `${this.url}${container}/${filename}?${generateBlobSas(signature, this.credential).toString()}`;
    }

    /**
     * @param {string} container 
     * @param {string} filename 
     * @param {object} data 
     */
    async writeJson(container, filename, data) {
        const s = new Readable();
        s._read = () => {};
        s.push(JSON.stringify(data));
        s.push(null);
        const containerClient = this.blobService.getContainerClient(container);
        const blockBlobClient = containerClient.getBlockBlobClient(filename);
        await blockBlobClient.uploadStream(s);
        return {
            filename: filename,
            container: container
        };
    }
}