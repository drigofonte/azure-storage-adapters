const { BlobSASPermissions, BlobServiceClient, ContainerSASPermissions, StorageSharedKeyCredential, generateBlobSASQueryParameters: generateBlobSas } = require('@azure/storage-blob');
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
     * @param {string} contentType The content type of the blob. Defaults to application/octet-stream
     */
    generateBlobSas(container, filename, validFor = 1, validForUnit = 'hour', contentType = 'application/octet-stream', permissions = 'r') {
        const start = moment().subtract(5, 'minute');
        const end = start.clone().add(validFor, validForUnit);

        const signature = {
            startsOn: start.toDate(),
            expiresOn: end.toDate(),
            permissions: BlobSASPermissions.parse(permissions).toString(),
            containerName: container,
            blobName: filename,
            contentType
        }
        return `${this.url}${container}/${filename}?${generateBlobSas(signature, this.credential).toString()}`;
    }

    /**
     * @param {string} container 
     * @param {Number} validFor The number of time units this blob's shared access signature should be valid for
     * @param {string} validForUnit MomentJS compatible unit of time (e.g. minute, hour, day, etc.)
     * @param {string} permissions
     */
    generateContainerSas(container, validFor = 1, validForUnit = 'hour', permissions = 'c') {
        const start = moment().subtract(5, 'minute');
        const end = start.clone().add(validFor, validForUnit);

        const signature = {
            startsOn: start.toDate(),
            expiresOn: end.toDate(),
            permissions: ContainerSASPermissions.parse(permissions).toString(),
            containerName: container
        }
        return `${this.url}${container}?${generateBlobSas(signature, this.credential).toString()}`;
    }

    /**
     * @param {string} container The name of the container to be created
     */
    async createContainer(container) {
        let containerClient = this.blobService.getContainerClient(container);
        const exists = await containerClient.exists();
        if (!exists) {
            await this.blobService.createContainer(container);
        }
        return containerClient;
    }

    /**
     * @param {string} container 
     * @param {string} filename 
     * @param {object} data 
     */
    async writeJson(container, filename, data) {
        return await this.writeString(container, filename, JSON.stringify(data));
    }

    /**
     * @param {string} container 
     * @param {string} filename 
     * @param {string} data 
     */
     async writeString(container, filename, data) {
      const s = new Readable();
      s._read = () => {};
      s.push(data);
      s.push(null);
      const containerClient = this.blobService.getContainerClient(container);
      const blockBlobClient = containerClient.getBlockBlobClient(filename);
      await blockBlobClient.uploadStream(s);
      return {
          filename: filename,
          container: container
      };
  }

    /**
     * @param {string} container 
     * @param {string} filename 
     * @param {Uint8Array} data 
     */
    async writeBuffer(container, filename, data) {
        const buffer = Buffer.from(data);
        const s = new Readable();
        s._read = () => {};
        s.push(buffer);
        s.push(null);
        const containerClient = this.blobService.getContainerClient(container);
        const blockBlobClient = containerClient.getBlockBlobClient(filename);
        await blockBlobClient.uploadStream(s);
        return {
            filename: filename,
            container: container
        };
    }

    /**
     * @param {String} container 
     * @param {String} filename 
     * @returns {String}
     */
    async readString(container, filename) {
      const containerClient = this.blobService.getContainerClient(container);
      const blobClient = containerClient.getBlobClient(filename);
      const response = await blobClient.download();
      const downloaded = (await streamToBuffer(response.readableStreamBody)).toString();

      async function streamToBuffer(readableStream) {
        return new Promise((resolve, reject) => {
          const chunks = [];
          readableStream.on("data", (data) => {
            chunks.push(data instanceof Buffer ? data : Buffer.from(data));
          });
          readableStream.on("end", () => {
            resolve(Buffer.concat(chunks));
          });
          readableStream.on("error", reject);
        });
      }

      return downloaded;
    }

    /**
     * @param {String} container 
     * @param {String} filename 
     * @returns {Object}
     */
    async readJson(container, filename) {
      return JSON.parse(await this.readString(container, filename));
    }
}