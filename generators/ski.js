// todo: convert to mariencoder format

const schema = require("../../../schemas");

const TWC_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525";
const mqtt = require("../../mqttlib")

async function getData(locations) {
    let fullData = ``;

    for (const loc of locations) {
       //console.log(locations)
        const res = await getSki(loc);
        const fileName = `${loc}-ski.py`
        await mqtt.i1_exec(res, `runomni /twc/util/loadSCMTconfig.pyc /home/dgadmin/${fileName}`, fileName, "i1/data/ski")
        mqtt.disconnect()
    }

    return true;
}

async function getSki(skiId) {
    const ccFetch = await fetch(
        `https://api.onthesnow.com/api/v2/resort/${skiId}/snowreport`,
        { timeout: 50 * 1000 }
    );

    if (!ccFetch.ok) return { error: "Getting ski conditions was NOT OK" };

    const ccData = await ccFetch.json();
    ccData.location = skiId;

    const i1_cc = schema.i1_res_ski(ccData);
    return i1_cc;
}

module.exports = getData;

// below is SCHEMA

const surfaceTypeMap = {
  0: '',
  1: 'Powder',
  2: 'Packed Powder',
  3: 'Machine Groomed',
  4: 'Hard Packed',
  5: 'Variable Conditions',
  6: 'Wet Snow',
  7: 'Spring Conditions',
  8: 'Corn Snow',
  9: 'Frozen Granular',
  10: 'Loose Granular',
  11: 'Ice',
  12: 'Man Made',
  13: 'Windblown',
  14: 'Crust',
  15: 'Rocks',
  16: 'Grass',
  17: 'Dirt'
};

const statusMap = {
  0: 'Closed',
  1: 'Open',
  2: 'Scheduled'
};

/**
 * Generate Python code to set current ski conditions based on resort data
 * @param {Object} input - OnTheSnow API resort object
 * @returns {string} - Python code block
 */
module.exports = function skiConditions(input) {
  const snow = input.snow || {};

  const snowDepths = [snow.base, snow.middle, snow.summit].filter(n => typeof n === 'number');
  const minSnow = snowDepths.length ? Math.min(...snowDepths) : 'None';
  const maxSnow = snowDepths.length ? Math.max(...snowDepths) : 'None';

  const minNewSnow = typeof snow.last24 === 'number' && snow.last24 > 0 ? `${snow.last24}` : '';
  const surfaceDesc = surfaceTypeMap[input.surfaceType] || '';

  const numTrailsOpen = typeof input.runs?.open === 'number' ? input.runs.open : 'None';
  const percentTerrainOpen = typeof input.runs?.openPercent === 'number' ? input.runs.openPercent : 'None';
  const shortOperatingStatus = statusMap[input.status?.openFlag] || 'None';

  const reportTime = Math.round(new Date(input.updatedAt).getTime() / 1000);
  const expireTime = Math.round(Date.now() / 1000) + (12 * 60 * 60);

  return `
import twccommon

#areaList = wxdata.getUGCInterestList('${input.slug}', 'obsStation')

twccommon.Log.info("RWE - Current Conditions is being sent")

#if (not areaList):
    #abortMsg()

#for area in areaList:
b = twc.Data()
b.minNewSnow = '${minNewSnow}'
b.minSnow = ${minSnow}
b.maxSnow = ${maxSnow}
b.surfaceDesc = '${surfaceDesc}'
b.numTrailsOpen = ${numTrailsOpen}
b.percentTerrainOpen = ${percentTerrainOpen}
b.shortOperatingStatus = '${shortOperatingStatus}'
b.reportTime = ${reportTime}

wxdata.setData('${input.location}', 'skiConditions', b, ${expireTime})
`;
};