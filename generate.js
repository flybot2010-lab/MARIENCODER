import fs from 'fs';
import path from 'path';
import axios from 'axios';
import yaml from 'yaml';
import exec from 'child_process';
import os from 'os';
import { fileURLToPath } from 'url';

const config = yaml.parse(fs.readFileSync('config.yaml', 'utf8'));
let pythonPrefix;
if (os.platform() === 'win32') {
  pythonPrefix = 'py'
} else {
  pythonPrefix = 'python3'
}
exec.execSync(`${pythonPrefix} ./fetch_remote_config.py`, { stdio: 'inherit' });

import { searchByObs } from './LFRecord.js';
import { searchByCoopId } from './LFRecord.js';
import { generateDaily } from "./generators/daily.js";
import { generateDaypart } from "./generators/daypartfcst.js";
import { generateHourly } from "./generators/hourly.js";
import { generateCurrent } from "./generators/current.js";

const API_KEY = config.API.WEATHER_API_KEY;
const AERO_API_KEY = "mTRBDG1YwVoonJLizwdx3TlArbIyzatA";
const units = config.API.UNITS;
const DATA_MINUTE_INTERVAL = config.SYSTEM.DATA_MINUTE_INTERVAL;
const NTP_MINUTE_INTERVAL = config.SYSTEM.NTP_MINUTE_INTERVAL;
const RADAR_MINUTE_INTERVAL = config.SYSTEM.RADAR_MINUTE_INTERVAL;

const interest_list = JSON.parse(fs.readFileSync('./remote/interest_lists.json', 'utf8'));
const codeConversion = JSON.parse(fs.readFileSync('code-conversion.json', 'utf8'));

const obs_interest_list = interest_list.obsStation;
const coop_interest_list = interest_list.coopId;
const county_interest_list = interest_list.county;
const airport_interest_list = interest_list.airport ?? []; // NEW: add "airport": ["KGRB", "KMKE", ...] to your interest_lists.json

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const OUTPUT_DIR = path.join(__dirname, 'output');
if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

// airport shit ==================
const wind_dir = {
    'Calm': 0, 'N': 1, 'NNE': 2, 'NE': 3, 'ENE': 4,
    'E': 5, 'ESE': 6, 'SE': 7, 'SSE': 8, 'S': 9,
    'SSW': 10, 'SW': 11, 'WSW': 12, 'W': 13, 'WNW': 14,
    'NW': 15, 'NNW': 16, 'Var': 17
};


function generateAirportConditions(input) {
    const reasons = input?.reasons ?? [];
    const airport = input.airport;
    let py = `
import twccommon
b = twc.Data()
`;
    reasons.forEach(r => {
        let delayType = "none";
        let trend = 0;
        if (r.reason.includes("departure")) delayType = "departure";
        if (r.reason.includes("arrival") || r.reason.includes("inbound")) delayType = "arrival";
        if (r.reason.includes("decreasing")) trend = 2;
        if (r.reason.includes("increasing")) trend = 1;
        py += `
b.${delayType}Delay = ${Math.round(r.delay_secs / 60)}
b.${delayType}DelayReason = '${r.category.charAt(0).toUpperCase() + r.category.slice(1)}'
b.${delayType}DelayTrend = ${trend}
wxdata.setData('${airport}', 'airportDelays', b, ${Math.floor(Date.now() / 1000) + 3600})
`;
    });
    if (input.weather) {
        py += `
b.temp = ${input.weather.temp_c ?? "None"}
b.skyCondition = ${input.weather.skyCondition ?? "None"}
b.windSpeed = ${input.weather.windSpeedKts ?? 0}
b.windDir = ${wind_dir[input.weather.windDir] ?? 0}
b.visibility = ${input.weather.visibilityKm ?? "None"}
`;
    }
    return py;
}

async function fetchFromAeroAPI(airportCode, apiKey) {
    try {
        const res = await axios.get(`https://aeroapi.flightaware.com/aeroapi/airports/${airportCode}`, {
            headers: { 'x-apikey': apiKey }
        });
        const data = res.data;
        return {
            airport: airportCode,
            weather: data.weather ? {
                temp_c: data.weather.temperature?.value,
                skyCondition: data.weather.weather_condition?.code,
                windSpeedKts: data.weather.wind_speed?.value,
                windDir: data.weather.wind_direction?.value,
                visibilityKm: data.weather.visibility?.value
            } : null,
            reasons: data.delays?.map(d => ({
                delay_secs: d.delay_seconds,
                reason: d.reason,
                category: d.category
            })) ?? []
        };
    } catch (err) {
        console.error("Failed to fetch AeroAPI data:", err.response?.data || err);
        return { airport: airportCode, reasons: [] };
    }
}
// ===========

function parsePILCodes(sameCode) {
  const mapping = codeConversion['codeToIStarPIL'];
  const result = mapping[sameCode];
  if (Array.isArray(result) && result.length > 0) {
    return result[0];
  }
  return 'UNKNOWN';
}

function capCanadaToUSSAME(eventType) {
  const mapping = codeConversion['cap_event-same_event'];
  const result = mapping[eventType];
  if (Array.isArray(result) && result.length >= 2) {
    return { same: result[0], pilExt: result[1] };
  }
  return { same: eventType, pilExt: '001' };
}

async function fetchDaily(lat, lon) {
  const url = "https://api.weather.com/v3/wx/forecast/daily/7day";
  const params = {
    geocode: `${lat},${lon}`,
    format: "json",
    units: units,
    language: "en-US",
    apiKey: API_KEY
  };
  const response = await axios.get(url, { params });
  return response.data;
}

async function fetchDaypart(lat, lon) {
  const url = "https://api.weather.com/v3/wx/forecast/daily/7day";
  const params = {
    geocode: `${lat},${lon}`,
    format: "json",
    units: units,
    language: "en-US",
    apiKey: API_KEY
  };
  const response = await axios.get(url, { params });
  return response.data;
}

async function fetchHourly(lat, lon) {
  const url = "https://api.weather.com/v3/wx/forecast/hourly/2day";
  const params = {
    geocode: `${lat},${lon}`,
    format: "json",
    units: units,
    language: "en-US",
    apiKey: API_KEY
  };
  const response = await axios.get(url, { params });
  return response.data;
}

async function fetchAlertHeadlines(lat, lon) {
  const url = "https://api.weather.com/v3/alerts/headlines";
  const params = {
    geocode: `${lat},${lon}`,
    format: "json",
    language: "en-US",
    apiKey: API_KEY
  };
  const response = await axios.get(url, { params });
  return response.data;
}

async function fetchAlertDetails(detailKey) {
  const url = "https://api.weather.com/v3/alerts/detail";
  const params = {
    alertId: detailKey,
    format: "json",
    language: "en-US",
    apiKey: API_KEY
  };
  try {
    const response = await axios.get(url, { params });
    return response.data;
  } catch (error) {
    console.error("Error fetching alert details:", error.message);
    return null;
  }
}

async function fetchCurrent(lat, lon) {
  const url = "https://api.weather.com/v3/wx/observations/current";
  const params = {
    geocode: `${lat},${lon}`,
    format: "json",
    units: units,
    language: "en-US",
    apiKey: API_KEY
  };
  const response = await axios.get(url, { params });
  return response.data;
}

async function aggregate() {
  let current = '';
  let hourly = '';
  let daily = '';
  let daypart = '';

  // Existing loops (unchanged)
  for (const obs of obs_interest_list) {
    try {
      const locData = await searchByObs(obs);
      if (!locData) {
        console.log(`Skipping ${obs} - not found in LFRecord`);
        continue;
      }
      const lat = locData.lat;
      const lon = locData.long;
      const wxData = await fetchCurrent(lat, lon);
      wxData.location = obs;
      wxData.county = locData.cntyId;
      const pyCode = generateCurrent(wxData);
      current += pyCode + '\n';
    } catch (err) {
      console.error(`Error generating current for ${obs}:`, err.message);
    }
  }

  for (const coopid of coop_interest_list) {
    try {
      const locData = await searchByCoopId(coopid);
      if (!locData) {
        console.log(`Skipping ${coopid} - not found in LFRecord`);
        continue;
      }
      const lat = locData.lat;
      const lon = locData.long;
      const dailyData = await fetchDaily(lat, lon);
      const daypartData = await fetchDaypart(lat, lon);
      const hourlyData = await fetchHourly(lat, lon);
      dailyData.location = coopid;
      daypartData.location = coopid;
      hourlyData.location = coopid;
      const dailyPy = generateDaily(dailyData);
      const daypartPy = generateDaypart(daypartData);
      const hourlyPy = generateHourly(hourlyData);
      daily += dailyPy + '\n';
      daypart += daypartPy + '\n';
      hourly += hourlyPy + '\n';
    } catch (err) {
      console.error(`Error generating forecasts for ${coopid}:`, err.message);
    }
  }

  for (const county of county_interest_list) {
    const primObs = obs_interest_list[0];
    const locData = await searchByObs(primObs);
    const lat = locData.lat;
    const lon = locData.long;
   
    try {
      let alertHeadlines = await fetchAlertHeadlines(lat, lon);
      if (alertHeadlines.alerts && alertHeadlines.alerts.length > 0) {
        const detailKey = alertHeadlines.alerts[0].detailKey;
        console.log(`Generating bulletin against assumed primary obs station ${primObs} (${lat},${lon}) for county ${county} via ${detailKey}`);
        const details = await fetchAlertDetails(detailKey);
        if (!details || !details.alertDetail || !details.alertDetail.texts || details.alertDetail.texts.length === 0) {
          console.log(`No alert details or texts found for ${detailKey}`);
          continue;
        }
       
        let alertExpirSec = 21600;
        if (alertHeadlines.alerts[0].expireTimeUtc) {
          alertExpirSec = alertHeadlines.alerts[0].expireTimeUtc - Math.floor(Date.now() / 1000);
        } else if (alertHeadlines.alerts[0].expireTimeLocal) {
          const expireDate = new Date(alertHeadlines.alerts[0].expireTimeLocal);
          alertExpirSec = Math.floor((expireDate.getTime() - Date.now()) / 1000);
        }
        alertExpirSec = Math.max(60, alertExpirSec);
       
        const texts = details.alertDetail.texts[0];
        const description = texts.description || "";
        const detailText = description.replace(/\s+/g, ' ').trim();
        const headlineText = alertHeadlines.alerts[0].headlineText || "";
        const officeName = alertHeadlines.alerts[0].officeName || "";
        const a_an = headlineText.length > 0 && 'aeiouAEIOU'.includes(headlineText[0]) ? "an" : "a";
        const bulletinText = `The ${officeName} has issued ${a_an} ${headlineText} ### ${detailText}`;
       
        if (alertHeadlines.alerts[0].countryCode == "US") {
          console.log("This alert is American.")
          const same = alertHeadlines.alerts[0].productIdentifier
          const pilCode = parsePILCodes(same);
          const load = `${county}|${same}|${pilCode}|${bulletinText}|${alertExpirSec}`
          await provisionIntelliStar("bulletin", load);
        } else if (alertHeadlines.alerts[0].countryCode == "CA") {
          console.log("This alert is Canadian.")
          const eventType = alertHeadlines.alerts[0].phenomena;
          const { same, pilExt } = capCanadaToUSSAME(eventType || "");
          const load = `${county}|${same}|${pilExt}|${bulletinText}|${alertExpirSec}`
          await provisionIntelliStar("bulletin", load);
        } else {
          console.log("This alert is neither American nor Canadian. Skipping.")
        }
      } else {
        console.log(`No alerts for county ${county}`);
      }
    } catch (err) {
      console.error(`Error generating bulletin for county ${county}:`, err.message);
    }
  }

  // ====================== NEW: Airport delays & conditions ======================
  let airportPy = '';
  for (const airportCode of airport_interest_list) {
    try {
      const input = await fetchFromAeroAPI(airportCode, AERO_API_KEY);
      const pyCode = generateAirportConditions(input);
      airportPy += pyCode + '\n';
      console.log(`Generated airport conditions for ${airportCode}`);
    } catch (err) {
      console.error(`Error generating airport conditions for ${airportCode}:`, err.message);
    }
  }
  // ============================================================================

  await fs.promises.writeFile(path.join(OUTPUT_DIR, 'current.py'), current);
  await fs.promises.writeFile(path.join(OUTPUT_DIR, 'daily.py'), daily);
  await fs.promises.writeFile(path.join(OUTPUT_DIR, 'daypart.py'), daypart);
  await fs.promises.writeFile(path.join(OUTPUT_DIR, 'hourly.py'), hourly);
  await fs.promises.writeFile(path.join(OUTPUT_DIR, 'airport.py'), airportPy); // NEW: airport.py

  console.log('All products written to output folder.');
}

async function provisionIntelliStar(job, bulletinParams="") {
  try {
    console.log("Starting provisioning to IntelliStar...");
    if (job === "bulletin" && bulletinParams) {
      exec.execSync(`${pythonPrefix} provision.py --job bulletin --bulletinParams "${bulletinParams}"`, { stdio: 'inherit' });
    } else {
      exec.execSync(`${pythonPrefix} provision.py --job ${job}`, { stdio: 'inherit' });
    }
    console.log("Provisioning completed successfully.");
  } catch (error) {
    console.error("Provisioning failed:", error);
  }
}

function countdown(dataSeconds, ntpSeconds, radarSeconds) {
  return new Promise((resolve) => {
    if (dataSeconds <= 0 || ntpSeconds <= 0 || radarSeconds <= 0) {
      resolve({
        dataRemaining: Math.max(0, dataSeconds),
        ntpRemaining: Math.max(0, ntpSeconds),
        radarRemaining: Math.max(0, radarSeconds)
      });
      return;
    }
   
    let dataRemaining = dataSeconds;
    let ntpRemaining = ntpSeconds;
    let radarRemaining = radarSeconds;
   
    const interval = setInterval(() => {
      const dataMins = Math.max(0, Math.floor(dataRemaining / 60));
      const dataSecs = dataRemaining >= 0 ? dataRemaining % 60 : 0;
      const ntpMins = Math.max(0, Math.floor(ntpRemaining / 60));
      const ntpSecs = ntpRemaining >= 0 ? ntpRemaining % 60 : 0;
      const radarMins = Math.max(0, Math.floor(radarRemaining / 60));
      const radarSecs = radarRemaining >= 0 ? radarRemaining % 60 : 0;
     
      process.stdout.write('\r\x1B[K');
      process.stdout.write(`Next data update: ${dataMins.toString().padStart(2, '0')}:${dataSecs.toString().padStart(2, '0')}\n`);
      process.stdout.write('\x1B[K');
      process.stdout.write(`Next time sync: ${ntpMins.toString().padStart(2, '0')}:${ntpSecs.toString().padStart(2, '0')}\n`);
      process.stdout.write('\x1B[K');
      process.stdout.write(`Next radar update: ${radarMins.toString().padStart(2, '0')}:${radarSecs.toString().padStart(2, '0')}`);
      process.stdout.write('\x1B[2A');
     
      dataRemaining--;
      ntpRemaining--;
      radarRemaining--;
      if (dataRemaining < 0 || ntpRemaining < 0 || radarRemaining < 0) {
        clearInterval(interval);
        process.stdout.write('\r\x1B[K\n\x1B[K\n\x1B[K\r');
        resolve({
          dataRemaining: Math.max(0, dataRemaining),
          ntpRemaining: Math.max(0, ntpRemaining),
          radarRemaining: Math.max(0, radarRemaining)
        });
      }
    }, 1000);
  });
}

async function runLoop() {
  let ntpCountdown = NTP_MINUTE_INTERVAL * 60;
  let radarCountdown = RADAR_MINUTE_INTERVAL * 60;
  let dataCountdown = DATA_MINUTE_INTERVAL * 60;
  console.log("Starting initial generation for all forecast products...");
  try {
    await aggregate();
  } catch (err) {
    console.error(`Error during aggregation:`, err);
  }
  await provisionIntelliStar("data");
  console.log("Data products generated and provisioned.");
 
  console.log("Starting initial NTP time synchronization...");
  await provisionIntelliStar("timesync");
  console.log("NTP time synchronization completed.");
 
  console.log("Starting initial radar data provisioning...");
  await provisionIntelliStar("radar");
  console.log("Radar data provisioning completed.");
 
  while (true) {
    console.log(`\nWaiting until next update...`);
    const remaining = await countdown(dataCountdown, ntpCountdown, radarCountdown);
    dataCountdown = remaining.dataRemaining;
    ntpCountdown = remaining.ntpRemaining;
    radarCountdown = remaining.radarRemaining;
   
    if (dataCountdown <= 0) {
      console.log("Starting generation for forecast products...");
      try {
        await aggregate();
      } catch (err) {
        console.error(`Error during aggregation:`, err);
      }
      await provisionIntelliStar("data");
      console.log("All products generated and provisioned.");
      dataCountdown = DATA_MINUTE_INTERVAL * 60;
    }
   
    if (ntpCountdown <= 0) {
      console.log("Starting NTP time synchronization...");
      await provisionIntelliStar("timesync");
      console.log("NTP time synchronization completed.");
      ntpCountdown = NTP_MINUTE_INTERVAL * 60;
    }
   
    if (radarCountdown <= 0) {
      console.log("Starting radar data provisioning...");
      await provisionIntelliStar("radar");
      console.log("Radar data provisioning completed.");
      radarCountdown = RADAR_MINUTE_INTERVAL * 60;
    }
  }
}

runLoop();