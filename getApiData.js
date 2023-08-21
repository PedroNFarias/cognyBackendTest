//consumir a API
const axios = require('axios');

const baseURL = 'https://datausa.io/api/data';

module.exports = {
    getData: (drilldownsParam, measuresParam) => axios.get(baseURL, {
        params: {
            drilldowns: drilldownsParam,
            measures : measuresParam
        }
    })
    .then(function (response) {
        return response.data.data;
    })
}