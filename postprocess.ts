import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import readline from 'readline';

import { globSync } from 'glob';
import { pick } from 'lodash';
import papa, { type ParseConfig } from 'papaparse';

const datasetDirs = globSync('./data/sec13f/*');
const cikNamesDirs = globSync('./data/cik-name-lookup');
const cikCusipDirs = globSync('./data/cik-cusip-lookup');
const fundsDir = './data/sec13f-funds';
const filingsDir = './data/sec13f-filings';
const holdingsDir = './data/sec13f-holdings';
const tmpHoldingsDir = './tmp/sec13f-holdings';
// NOTE: Use for debugging
// const filingsDir = './data/sec13f-filings__deleteme';
// const holdingsDir = './data/sec13f-holdings__deleteme';

const safeJsonParse = (content: string) => {
  let data: any = null;
  try {
    data = JSON.parse(content);
    return { data, err: null };
  } catch (err) {
    return { data, err };
  }
};

const readFileStreamLines = (
  filepath: string,
  options?: { onLine?: (line: string, index: number) => void | Promise<void>; onEnd?: () => void }
) => {
  const fileStream = fs.createReadStream(filepath);

  const reader = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity, // Handle CRLF and LF the same way
  });

  let lineIndex = 0;
  reader.on('line', async (line) => {
    reader.pause(); // Pause reading

    const currLineIndex = lineIndex;
    lineIndex++;

    // Process the line as needed
    await options?.onLine?.(line, currLineIndex);

    reader.resume(); // Resume reading once the async operation is completed
  });

  const closePromise = new Promise<number>((res, rej) => {
    reader.on('close', () => {
      console.log(`Finished reading file ${filepath}`);
      options?.onEnd?.();
      // Return from promise the total number of processed rows
      res(lineIndex);
    });
  });

  return closePromise;
};

const loadDataset = async (datasetDir: string, options?: { verbose?: boolean }) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Loading dataset '${datasetDir}'`);
  const datasetFiles = (await fsp.readdir(datasetDir, 'utf-8')).filter((p) => !p.startsWith('.'));
  logger.log(
    `Found ${datasetFiles.length} files in dataset '${datasetDir}'. \nExample: '${datasetFiles[0]}'`
  );

  return datasetFiles;
};

const loadDatasetFile = async (
  datasetDir: string,
  filepath: string,
  options?: { verbose?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Reading file '${filepath}' of dataset '${datasetDir}'`);
  const fileContent = await fsp.readFile(filepath, 'utf-8');
  logger.log(`File '${filepath}' of dataset '${datasetDir}' has size ${fileContent.length}`);

  return fileContent;
};

const loadDatasetCsv = async (
  datasetDir: string,
  filepath: string,
  options?: {
    onRow?: (data: Record<string, any>, index: number) => unknown | Promise<unknown>;
    verbose?: boolean;
    ignoreErrors?: boolean;
    parseConfig?: ParseConfig;
  }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Reading CSV '${filepath}' of dataset '${datasetDir}'`);

  // NOTE:
  // First approach was to plainly load the file with `fs.readFile`, but soon
  // we hit `ERR_STRING_TOO_LONG`.
  //
  // Next, as a workaround, we loaded the data as Buffer, split the CSV to smaller chunks,
  // and then loaded the chunks one by one. (See https://github.com/nodejs/node/issues/9489#issuecomment-279889904)
  // But then we've hit `ERR_FS_FILE_TOO_LARGE`, as this approach works only with up to 2GB.
  //
  // Current approach is to stream the file contents line by line to avoid these limits.
  let headers = '';
  const totalRows = await readFileStreamLines(filepath, {
    onLine: async (line, index) => {
      const trimmedLine = line.trim();
      if (!trimmedLine) return;

      if (!headers) {
        headers = trimmedLine;
        console.log({ headers });
        return;
      }

      logger.log(`Parsing CSV row ${index + 1} of file '${filepath}' of dataset '${datasetDir}'`);

      const lineWithHeaders = [headers, trimmedLine].join('\n');
      const { data, errors } = papa.parse(lineWithHeaders, {
        ...options?.parseConfig,
        header: true,
      });
      const error = errors[0];
      if (error) {
        const lineMsg = error.row == null ? '' : `at line ${index + 1} `;
        const causeMsg =
          error.row == null
            ? ''
            : `\n\nThe error above was caused by the following row:\n${trimmedLine}`; // prettier-ignore
        const errMsg =
          `CSV parsing error ${lineMsg}of file '${filepath}' of dataset '${datasetDir}':\n` +
          JSON.stringify(error) +
          causeMsg;

        if (options?.ignoreErrors) console.error(errMsg);
        else throw Error(errMsg);
      }

      await options?.onRow?.(data[0], index);
    },
  });

  logger.log(`CSV '${filepath}' of dataset '${datasetDir}' has ${totalRows} rows`);
};

const loadJson = (content: string, options?: { ignoreJsonError?: boolean }) => {
  const res = safeJsonParse(content);
  if (res.err) {
    if (!options?.ignoreJsonError) {
      throw res.err;
    } else {
      console.error(res.err);
    }
  }
  return res;
};

const mergePartialFiles = async (datasetDir: string, options?: { verbose?: boolean }) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Searching dataset '${datasetDir}' for partial files`);

  // 1. Load the dataset from filesystem
  const datasetFiles = await loadDataset(datasetDir, options);

  // 2. Merge partial files
  const partialFiles: Record<
    string, // multipartId
    { filepath: string; partIndex: number }[]
  > = {};

  logger.log(`Collecting info on partial files of dataset '${datasetDir}'`);

  // 2.1 First find all partial files. To keep memory requirements low,
  // we won't load ALL the files into memory, but instead only open them one by one,
  // check which ones are partial files, then close them, and then do the mergin
  // in the next step.
  for (const filename of datasetFiles) {
    const filepath = path.join(datasetDir, filename);
    const fileContent = await loadDatasetFile(datasetDir, filepath, options);

    if (fileContent.includes('_multipartId')) {
      logger.log(`File '${filepath}' of dataset '${datasetDir}' IS a partial file`);
    } else {
      logger.log(`Skipping file '${filepath}' of dataset '${datasetDir}' - NOT a partial file`);
      continue;
    }

    const { data } = loadJson(fileContent);
    const multipartId = data._multipartId;
    const partIndex = data._partIndex;

    if (!multipartId) {
      throw Error(
        `Invalid value for 'multipartId': ${multipartId} in file '${filepath}' of dataset '${datasetDir}'`
      );
    }
    if (typeof partIndex !== 'number' || partIndex < 0) {
      throw Error(
        `Invalid value for 'partIndex': ${partIndex} in file '${filepath}' of dataset '${datasetDir}'`
      );
    }

    logger.log(
      `Found part file ${partIndex} for multipart ID '${multipartId}' file '${filepath}' of dataset '${datasetDir}'`
    );

    if (!partialFiles[multipartId]) partialFiles[multipartId] = [];

    partialFiles[multipartId].push({ filepath, partIndex });
  }

  const partsCount = Object.values(partialFiles).reduce((agg, parts) => agg + parts.length, 0);
  const [examplePartName, exapleParts] = [...Object.entries(partialFiles)][0] ?? [];
  logger.log(`Done collecting info on partial files of dataset '${datasetDir}'`);
  logger.log(
    `Found ${Object.values(partialFiles).length} multipart files split across ${partsCount} parts` +
      `\nExample: '${examplePartName}': ${JSON.stringify(exapleParts)}`
  );

  logger.log(`Merging multipart files of dataset '${datasetDir}'`);

  // 2.2 Then, for each multi-part file, merge the parts in the correct
  //     order, write it to a common file, and delete the part files.
  for (const [multipartId, parts] of Object.entries(partialFiles)) {
    if (parts.length < 2) continue;

    logger.log(`Merging multipart file '${multipartId}' of dataset '${datasetDir}'`);

    const mergedFileName = path.join(datasetDir, `${multipartId}.json`);

    const sortedParts = parts
      .slice()
      .sort((a, b) => (a.partIndex > b.partIndex ? 1 : a.partIndex < b.partIndex ? -1 : 0));

    for (const part of sortedParts) {
      const partName = `index: '${part.partIndex}' file: '${part.filepath}'`;
      const logSuffix = ` of multipart file '${multipartId}' of dataset '${datasetDir}'`;
      logger.log(`Reading part ${partName}${logSuffix}`);
      const partJsonContent = await fsp.readFile(part.filepath, 'utf-8');

      logger.log(`Extracting content from part ${partName}${logSuffix}`);
      const partJson = JSON.parse(partJsonContent);
      const partContent = partJson._partData;

      logger.log(`Appending part ${partName} to file '${mergedFileName}'${logSuffix}`);
      await fsp.appendFile(mergedFileName, partContent, 'utf-8');
    }

    logger.log(
      `Verifying validity of merged multipart file '${multipartId}' of dataset '${datasetDir}'`
    );
    const mergedContent = await loadDatasetFile(datasetDir, mergedFileName, options);
    JSON.parse(mergedContent);

    // Delete part files
    for (const part of sortedParts) {
      const partName = `index: '${part.partIndex}' file: '${part.filepath}'`;
      logger.log(
        `Deleting part ${partName} of multipart file '${multipartId}' of dataset '${datasetDir}'`
      );
      await fsp.unlink(part.filepath);
    }

    logger.log(
      `Done deleting part files for multipart '${multipartId}' of dataset '${datasetDir}'`
    );
  }
};

const mergeAmendmentFiles = async (
  datasetDir: string,
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Searching dataset '${datasetDir}' for amended filings`);

  // 1. Load the dataset from filesystem
  const datasetFiles = await loadDataset(datasetDir, options);

  // 2. Delete filings that have been "amended" (aka overwriten) by newer versions
  const amendedFilings: Set<string> = new Set();
  const filingIdToFilename: Record<string, string[]> = {};

  logger.log(`Collecting info on amended filings of dataset '${datasetDir}'`);

  // 2.1 First find all amended files. To keep memory requirements low,
  // we won't load ALL the files into memory, but instead only open them one by one,
  // check which ones are amendments, then close them, and then do the mergin
  // in the next step.
  for (const filename of datasetFiles) {
    const filepath = path.join(datasetDir, filename);
    const fileContent = await loadDatasetFile(datasetDir, filepath, { verbose: false });

    // TODO - CHECK IF THIS WORKS? We want to catch "example"/"exhibit" files
    if (fileContent.toLowerCase().includes('"ex-')) {
      throw filepath;
    }

    const { data: fileJson, err: jsonErr } = loadJson(fileContent, options);
    if (jsonErr) continue;

    const filingId = fileJson.file_number;

    if (!filingIdToFilename[filingId]) filingIdToFilename[filingId] = [];
    filingIdToFilename[filingId].push(filepath);

    if (fileJson.amendment_type) {
      logger.log(
        `File '${filepath}' (ID: ${filingId}) of dataset '${datasetDir}' IS an amendment # ${fileJson.amendment_number} (type: ${fileJson.amendment_type})`
      );
      amendedFilings.add(filingId);
    } else {
      logger.log(
        `Skipping file '${filepath}' (ID: ${filingId}) of dataset '${datasetDir}' - NOT an amendment file`
      );
      continue;
    }
  }

  const amendmentFiles = [...amendedFilings].map((filingId) => ({
    filingId,
    filepaths: filingIdToFilename[filingId],
  }));
  const totalVersionsCount = [...amendmentFiles].reduce((agg, f) => agg + f.filepaths.length, 0);
  const amendmentFile = amendmentFiles[0];
  logger.log(`Done collecting info on amendment files of dataset '${datasetDir}'`);
  logger.log(
    `Found ${amendmentFiles.length} amended files split across ${totalVersionsCount} versions` +
      `\nExample: '${amendmentFile.filingId}': ${JSON.stringify(amendmentFile.filepaths)}`
  );

  logger.log(`Merging amendment files of dataset '${datasetDir}'`);

  // 2.2 Then, for each multi-part file, merge the parts in the correct
  //     order, write it to a common file, and delete the part files.
  for (const amendment of amendmentFiles) {
    const { filingId, filepaths } = amendment;

    if (filepaths.length < 2) continue;

    logger.log(`Merging amendment files of filing '${filingId}' of dataset '${datasetDir}'`);

    const filings: { data: any; filepath: string }[] = [];
    for (const filepath of filepaths) {
      const fileContent = await loadDatasetFile(datasetDir, filepath, { verbose: false });

      const { data, err } = loadJson(fileContent, options);
      if (err) continue;

      filings.push({ data, filepath });
    }

    // Sort by date - oldest first
    filings.sort((a, b) => {
      // First sort based on date, and in case of same dates, use the entry that
      // higher ID
      const aa = `${a.data.report_date}_${a.data.externalId}`;
      const bb = `${b.data.report_date}_${b.data.externalId}`;
      return aa > bb ? 1 : aa < bb ? -1 : 0;
    });

    // We take the latest filing, as all amendments should contain all information
    const filing = filings.pop()!;

    filing.data.previousVersions = filings.map((f) => ({
      externalId: f.data.externalId,
      report_date: f.data.report_date,
      directoryUrl: f.data.directoryUrl,
    }));

    const filingDataContent = JSON.stringify(filing.data);
    logger.log(`Updating file ${filing.filepath}`);
    await fsp.writeFile(filing.filepath, filingDataContent, 'utf-8');

    // Delete the old filings
    for (const oldFiling of filings) {
      const partName = `extID: '${oldFiling.data.externalId}' file_number: '${oldFiling.data.file_number}'`;
      logger.log(`Deleting superseded filing ${oldFiling.filepath} ${partName}`);
      await fsp.unlink(oldFiling.filepath);
    }

    logger.log(
      `Done deleting superseded filing files for filing '${filingId}' of dataset '${datasetDir}'.` +
        ` Latest version ${filing.filepath} url: ${filing.data.directoryUrl}`
    );
  }
};

// Dataset taken from https://github.com/leoliu0/cik-cusip-mapping/blob/master/cik-cusip-maps.csv
// See https://github.com/leoliu0/cik-cusip-mapping
const prepareCikCusip = async (options?: { verbose?: boolean }) => {
  const logger = options?.verbose ? console : { log: () => {} };

  const cusip8ToCik: Record<string, string> = {};
  const cikToCusip8: Record<string, string> = {};

  for (const cikCusipDir of cikCusipDirs) {
    const cikCusipFiles = await loadDataset(cikCusipDir, options);

    for (const cikCusipFilename of cikCusipFiles) {
      const cikCusipFilepath = path.join(cikCusipDir, cikCusipFilename);
      await loadDatasetCsv(cikCusipDir, cikCusipFilepath, {
        ...options,
        parseConfig: {
          delimiter: ',',
          header: true,
          skipEmptyLines: true,
        },
        // cik,cusip6,cusip8
        // 828119.0,88343A,88343A10
        onRow: (row) => {
          cusip8ToCik[row.cusip8] = row.cik;
          cikToCusip8[row.cik] = row.cusip8;
        },
      });
    }
  }

  const totalCikCount = Object.keys(cusip8ToCik).length;
  const [exampleCusip8, exampleCik] = Object.entries(cusip8ToCik)[0];
  logger.log(`Done collecting info on CUSIP-to-CIK associations`);
  logger.log(`Found ${totalCikCount} CIKs\nExample: '${exampleCusip8}': ${exampleCik}`);

  return { cusip8ToCik, cikToCusip8 };
};

const assocCikAndCusip = async (
  datasetDir: string,
  lookups: {
    cusip8ToCik: Record<string, string>;
    cikToCusip8: Record<string, string>;
  },
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Associating CUSIPs with CIKs for dataset '${datasetDir}'`);

  // 1. Load the dataset from filesystem
  const datasetFiles = await loadDataset(datasetDir, options);

  // 2. Add data on all company names associated with current filing's CIK
  for (const filename of datasetFiles) {
    const filepath = path.join(datasetDir, filename);

    logger.log(`Associating CUSIPs with CIKs of filing '${filepath}' of dataset '${datasetDir}'`);

    const fileContent = await loadDatasetFile(datasetDir, filepath, { ...options, verbose: false });
    const { data, err } = loadJson(fileContent, options);
    if (err) continue;

    // The fund org is ID'd by CIK, so we add the cusip8
    const cikNum = Number.parseInt(data.cik);
    data.cusip8 = lookups.cikToCusip8[cikNum];

    // Holdings are ID'd by CUSIP9, so we convert them to CUSIP8 and add CIKs
    if (data.holdings) {
      data.holdings = data.holdings.map((holding: any) => {
        const cusip9: string = holding.cusip;
        const cusip8 = cusip9.slice(0, -1); // Simply drop last char to get CUSIP8
        if (lookups.cusip8ToCik[cusip8]) {
          const cik = Number.parseInt(lookups.cusip8ToCik[cusip8]);
          holding.cik = cik.toString().padStart(10, '0');
        } else {
          holding.cik = null;
        }

        return holding;
      });
    }

    const filingDataContent = JSON.stringify(data);
    logger.log(`Updating file ${filepath}`);
    await fsp.writeFile(filepath, filingDataContent, 'utf-8');
  }
  logger.log(`Done enriching filings with CIKs of dataset '${datasetDir}'.`);
};

const addCompanyNames = async (
  datasetDir: string,
  cikToCompanyNames: Record<string, string[]>,
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Associating CIK with company names for dataset '${datasetDir}'`);

  // 1. Load the dataset from filesystem
  const datasetFiles = await loadDataset(datasetDir, options);

  // 2. Add data on all company names associated with current filing's CIK
  for (const filename of datasetFiles) {
    const filepath = path.join(datasetDir, filename);

    logger.log(
      `Associating CIK with company names of filing '${filepath}' of dataset '${datasetDir}'`
    );

    const fileContent = await loadDatasetFile(datasetDir, filepath, { verbose: false });
    const { data, err } = loadJson(fileContent, options);
    if (err) continue;

    data.companyNames = cikToCompanyNames[data.cik];

    // NOTE: Ensure you've already ran `assocCikAndCusip` before `addCompanyNames`,
    // so we can associate holding companies with company names too.
    if (data.holdings) {
      data.holdings = data.holdings.map((holding: any) => {
        const cik: string = holding.cik;
        holding.companyNames = cikToCompanyNames[cik] ?? null;
        return holding;
      });
    }

    const filingDataContent = JSON.stringify(data);
    logger.log(`Updating file ${filepath}`);
    await fsp.writeFile(filepath, filingDataContent, 'utf-8');
  }
  logger.log(`Done enriching filings with company names of dataset '${datasetDir}'.`);
};

// Dataset taken from https://www.sec.gov/Archives/edgar/cik-lookup-data.txt
// See "CIK" section in  https://www.sec.gov/os/accessing-edgar-data
const prepareCompanyNames = async (options?: { verbose?: boolean }) => {
  const logger = options?.verbose ? console : { log: () => {} };

  const cikToCompanyNames: Record<string, string[]> = {};

  for (const cikNamesDir of cikNamesDirs) {
    const cikToCompNameFiles = await loadDataset(cikNamesDir, options);

    for (const cikNamesFilename of cikToCompNameFiles) {
      const cikNamesFilepath = path.join(cikNamesDir, cikNamesFilename);
      const fileContent = await loadDatasetFile(cikNamesDir, cikNamesFilepath, options);
      const fileLines = fileContent.split('\n').map((line, index) => ({ line, lineno: index + 1 }));

      for (const { line, lineno } of fileLines) {
        // Ignore empty lines
        if (!line.trim()) continue;

        // E.g `!J INC:0001438823:`, so `<CompName>:<CIK>:`
        // - CompName: `!J INC`
        // - CIK: 0001438823
        // - If any extra info, we throw error
        // CompName MAY include extra `:`. Hence we parse from the end.
        const lineParts = line.split(':');
        // NOTE: Line should end with `:`, so last part should be empty
        const lastPart = (lineParts.pop() || '').trim();

        if (lastPart) {
          throw Error(
            `Unexpected content found at line ${lineno} in ${cikNamesFilepath} for CIK-Company associatons: '${lastPart}'`
          );
        }

        const cik = (lineParts.pop() || '').trim();
        if (!cik.match(/\d+/)) {
          throw Error(
            `Invalid value for CIK found at line ${lineno} in ${cikNamesFilepath} for CIK-Company associatons: '${cik}'`
          );
        }

        const compName = lineParts.join(':').trim();

        if (!cikToCompanyNames[cik]) cikToCompanyNames[cik] = [];
        cikToCompanyNames[cik].push(compName);
      }
    }
  }

  const totalCikCount = Object.keys(cikToCompanyNames).length;
  const totalCompNamesCount = Object.values(cikToCompanyNames).reduce(
    (agg, f) => agg + f.length,
    0
  );
  const [exampleCik, exampleCikCompNames] = Object.entries(cikToCompanyNames)[0];
  logger.log(`Done collecting info on CIK-to-CompName associations`);
  logger.log(
    `Found ${totalCikCount} CIKs with total of ${totalCompNamesCount} company names` +
      `\nExample: '${exampleCik}': ${JSON.stringify(exampleCikCompNames)}`
  );

  return cikToCompanyNames;
};

// TODO?
// Dataset taken from https://www.sec.gov/files/company_tickers_exchange.json
// See "CIK, ticker, and exchange associations" section in  https://www.sec.gov/os/accessing-edgar-data
const addTickers = async (options?: { verbose?: boolean }) => {
  const logger = options?.verbose ? console : { log: () => {} };

  const cikToCompanyNames: Record<string, string[]> = {};

  for (const cikNamesDir of cikNamesDirs) {
    const cikToCompNameFiles = await loadDataset(cikNamesDir, options);

    for (const cikNamesFilename of cikToCompNameFiles) {
      const cikNamesFilepath = path.join(cikNamesDir, cikNamesFilename);
      const fileContent = await loadDatasetFile(cikNamesDir, cikNamesFilepath, options);
      const fileLines = fileContent.split('\n').map((line, index) => ({ line, lineno: index + 1 }));

      for (const { line, lineno } of fileLines) {
        // Ignore empty lines
        if (!line.trim()) continue;

        // E.g `!J INC:0001438823:`, so `<CompName>:<CIK>:`
        // - CompName: `!J INC`
        // - CIK: 0001438823
        // - If any extra info, we throw error
        // CompName MAY include extra `:`. Hence we parse from the end.
        const lineParts = line.split(':');
        // NOTE: Line should end with `:`, so last part should be empty
        const lastPart = (lineParts.pop() || '').trim();

        if (lastPart) {
          throw Error(
            `Unexpected content found at line ${lineno} in ${cikNamesFilepath} for CIK-Company associatons: '${lastPart}'`
          );
        }

        const cik = (lineParts.pop() || '').trim();
        if (!cik.match(/\d+/)) {
          throw Error(
            `Invalid value for CIK found at line ${lineno} in ${cikNamesFilepath} for CIK-Company associatons: '${cik}'`
          );
        }

        const compName = lineParts.join(':').trim();

        if (!cikToCompanyNames[cik]) cikToCompanyNames[cik] = [];
        cikToCompanyNames[cik].push(compName);
      }
    }
  }

  const totalCikCount = Object.keys(cikToCompanyNames).length;
  const totalCompNamesCount = Object.values(cikToCompanyNames).reduce(
    (agg, f) => agg + f.length,
    0
  );
  const [exampleCik, exampleCikCompNames] = Object.entries(cikToCompanyNames)[0];
  logger.log(`Done collecting info on CIK-to-CompName associations`);
  logger.log(
    `Found ${totalCikCount} CIKs with total of ${totalCompNamesCount} company names` +
      `\nExample: '${exampleCik}': ${JSON.stringify(exampleCikCompNames)}`
  );

  return cikToCompanyNames;
};

const iterDataset = async (
  datasetDir: string,
  onData: (ctx: { data: any; filepath: string; index: number }) => Promise<void>,
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Accessing data from dataset '${datasetDir}'`);

  // 1. Load the dataset from filesystem
  const datasetFiles = await loadDataset(datasetDir, options);

  // 2. Add data on all company names associated with current filing's CIK
  let index = 0;
  for (const filename of datasetFiles) {
    const filepath = path.join(datasetDir, filename);

    logger.log(`Accessing data from file '${filepath}' of dataset '${datasetDir}'`);

    const fileContent = await loadDatasetFile(datasetDir, filepath, { verbose: false });

    if (filepath.toLowerCase().endsWith('.json')) {
      const { data, err } = loadJson(fileContent, options);
      if (err) continue;
      await onData({ data, filepath, index });
    } else if (filepath.toLowerCase().endsWith('.csv')) {
      throw Error('Not implemnted!');
    } else {
      throw Error('Not implemnted!');
    }

    index++;
  }
  logger.log(`Done accessning data from dataset '${datasetDir}'.`);
};

const extractJsonSingleFile = async (
  datasetDir: string,
  outdir: string,
  extract: (ctx: {
    data: any;
    filepath: string;
    index: number;
  }) => Promise<{ id: string; data: any } | { id: string; data: any }[]>,
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  await iterDataset(
    datasetDir,
    async ({ data, filepath, index }) => {
      logger.log(`Extracting data from file '${filepath}' from dataset '${datasetDir}'`);

      const outdata = await extract({ data, filepath, index });

      const normOutdata = Array.isArray(outdata) ? outdata : [outdata];

      for (const { id: entryId, data: entryData } of normOutdata) {
        const entryContent = JSON.stringify(entryData);
        const entryFilepath = path.join(outdir, `${entryId}.json`);

        logger.log(`Writing file ${entryFilepath}`);
        await fsp.mkdir(outdir, { recursive: true });
        await fsp.writeFile(entryFilepath, entryContent, 'utf-8');
      }
    },
    options
  );
  logger.log(`Done extracting data from from dataset '${datasetDir}'.`);
};

const extractCSV = async (
  input: {
    datasetDir: string;
    name: string;
    outdir: string;
    extract: (ctx: {
      data: any;
      filepath: string;
      index: number;
    }) => Promise<Record<string, any> | Record<string, any>[]>;
  },
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const { datasetDir, name, outdir, extract } = input;

  const logger = options?.verbose ? console : { log: () => {} };

  const entryFilepath = path.join(outdir, `${name}.csv`);

  let headers: string[] = [];
  let csvData: string[] = [];

  // TODO - Use CSV WRITER
  const writeChunksToFile = async () => {
    logger.log(`Appending file ${entryFilepath}`);
    await fsp.mkdir(outdir, { recursive: true });

    for (const chunk of csvData) {
      await fsp.appendFile(entryFilepath, '\n' + chunk, 'utf-8');
    }

    csvData = [];
  };

  await iterDataset(
    datasetDir,
    async ({ data, filepath, index }) => {
      logger.log(`Extracting data from file '${filepath}' from dataset '${datasetDir}'`);

      const entryData = await extract({ data, filepath, index });

      const normEntries = Array.isArray(entryData) ? entryData : [entryData];

      for (const entry of normEntries) {
        const includeHeaders = !headers.length;
        const serialized = papa.unparse([entry], { header: includeHeaders });
        csvData.push(serialized);

        if (includeHeaders) {
          headers = Object.keys(entry);
        }

        if (index && index % 1000 == 0) await writeChunksToFile();
      }
    },
    { ...options, verbose: false }
  );

  await writeChunksToFile();

  logger.log(`Done extracting data from from dataset '${datasetDir}'.`);
};

const extractFunds = async (
  datasetDir: string,
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Extracting funds info from dataset '${datasetDir}'`);

  await extractCSV(
    {
      datasetDir,
      name: 'funds',
      outdir: fundsDir,
      extract: async ({ data, filepath, index }) => {
        logger.log(`Extracting funds from file '${filepath}' from dataset '${datasetDir}'`);

        const fields = [
          'companyName',
          'companyNames',
          'cik',
          'street1',
          'street2',
          'city',
          'state_or_country',
          'zip_code',
        ];
        const entry = pick(data, ...fields);

        return entry;
      },
    },
    { ...options, verbose: false }
  );
  logger.log(`Done extracting funds info from dataset '${datasetDir}'.`);
};

const extractFiling = async (
  datasetDir: string,
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Extracting filing info from dataset '${datasetDir}'`);

  await extractCSV(
    {
      datasetDir,
      name: 'filings',
      outdir: filingsDir,
      extract: async ({ data, filepath, index }) => {
        logger.log(`Extracting filings from file '${filepath}' from dataset '${datasetDir}'`);

        const fields = [
          'externalId',
          'formType',
          'dateFiled',
          'fullSubmissionUrl',
          'directoryUrl',
          'secIndexUrl',
          'xmlUrls',
          'report_date',
          'other_included_managers_count',
          'holdings_count_reported',
          'holdings_value_reported',
          'confidential_omitted',
          'report_type',
          'amendment_type',
          'amendment_number',
          'file_number',
          'other_managers',
        ];

        const entry = pick(data, ...fields);
        entry.xmlUrls = JSON.stringify(entry.xmlUrls ?? []);
        entry.other_managers = JSON.stringify(entry.other_managers ?? []);
        entry.fundCik = data.cik;
        entry.fundCompanyName = data.companyName;

        return entry;
      },
    },
    { ...options, verbose: false }
  );
  logger.log(`Done extracting filing info from dataset '${datasetDir}'.`);
};

const extractHoldings = async (
  datasetDir: string,
  options?: { verbose?: boolean; ignoreJsonError?: boolean }
) => {
  const logger = options?.verbose ? console : { log: () => {} };

  logger.log(`Extracting holdings info from dataset '${datasetDir}'`);

  await extractCSV(
    {
      datasetDir,
      name: 'holdings',
      outdir: holdingsDir,
      extract: async ({ data, filepath, index }) => {
        logger.log(`Extracting holdings from file '${filepath}' from dataset '${datasetDir}'`);

        const fields = [
          'cusip',
          'issuerName',
          'classTitle',
          'value',
          'sharesOrPrincipalAmount',
          'sharesOrPrincipalAmountType',
          'optionType',
          'investmentDiscretion',
          'otherManager',
          'votingAuthoritySole',
          'votingAuthorityShared',
          'votingAuthorityNone',
          // Custom
          'valueSharesFraction',
        ];

        const holdings = data.holdings || [];
        const sumHoldingsValue = holdings.reduce((agg: number, hold: any) => {
          // Ignore non-share entries
          if (hold.sharesOrPrincipalAmountType?.trim().toLowerCase() === 'sh') {
            return agg + hold.value;
          }
          return agg;
        }, 0);

        for (const holding of holdings) {
          holding.valueSharesFraction =
            Math.round(10_000 * (holding.value / sumHoldingsValue)) / 10_000;
        }

        return (data.holdings || []).map((holding: any) => {
          const entry = pick(holding, ...fields);
          entry.fundCik = data.cik;
          entry.fundCompanyName = data.companyName;
          entry.filingExternalId = data.externalId;
          entry._pk = `${data.externalId}__${holding.cusip}`;

          return entry;
        });
      },
    },
    { ...options, verbose: true }
  );
  logger.log(`Done extracting holdings info from dataset '${datasetDir}'.`);
};

// See https://stackoverflow.com/a/18650828/9788634
const _formatBytes = (bytes: number, decimals = 2) => {
  if (!+bytes) return '0 Bytes';

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
};

const _createCsvWriter = (file: string, logger: Pick<Console, 'log'>) => {
  const data: string[] = [];
  const headers: string[] = [];
  let sizeEstimate = 0;

  const push = (newData: any[]) => {
    const includeHeaders = !headers.length;
    const serialized = papa.unparse(newData, { header: includeHeaders });
    data.push(serialized);

    if (includeHeaders) {
      headers.push(...Object.keys(newData[0]));
    }
  };

  const flush = async () => {
    const newSizeEstimate = data.reduce((sum, chunk) => sum + chunk.length, sizeEstimate);
    logger.log(`Appending file ${file} (${_formatBytes(newSizeEstimate)})`);
    await fsp.mkdir(path.dirname(file), { recursive: true });

    for (const chunk of data) {
      sizeEstimate = chunk.length;

      await fsp.appendFile(file, '\n' + chunk, 'utf-8');
    }

    data.splice(0, data.length); // Clear whole array
  };

  const flushSync = () => {
    const newSizeEstimate = data.reduce((sum, chunk) => sum + chunk.length, sizeEstimate);
    logger.log(`Appending file ${file} (${_formatBytes(newSizeEstimate)})`);
    fs.mkdirSync(path.dirname(file), { recursive: true });

    for (const chunk of data) {
      sizeEstimate = chunk.length;

      fs.appendFileSync(file, '\n' + chunk, 'utf-8');
    }

    data.splice(0, data.length); // Clear whole array
  };

  return { data, headers, push, flush, flushSync };
};

const extractValueDeltaFromFilings = async (options?: {
  verbose?: boolean;
  ignoreErrors?: boolean;
}) => {
  const logger = options?.verbose ? console : { log: () => {} };

  const outFile = './data/sec13f-valuedelta/valuedelta.csv';
  const datesFile = './data/sec13f-reportdates/reportdates.csv';

  const parseConfig = {
    delimiter: ',',
    skipEmptyLines: true,
  };

  // 1. Load ALL filings
  const filingDir = './data/sec13f-filings';
  const filingsFiles = await loadDataset(filingDir, options);

  // 2. Prepare filings as { externalId: report_date }
  // TODO - Why am I getting results only from report dates 2011-2017???
  const filingReportDates: Record<string, string> = {};
  const allReportDates: Set<string> = new Set();
  for (const filename of filingsFiles) {
    const file = path.join(filingsDir, filename);
    await loadDatasetCsv(filingDir, file, {
      ...options,
      parseConfig,
      onRow: (row, index) => {
        if (index === 0) {
          console.log(`Filing example 1: ${JSON.stringify(row)}`);
        }

        filingReportDates[row.externalId] = row.report_date;
        allReportDates.add(row.report_date);
      },
    });
  }

  // Sort the report dates from oldest to newest
  const sortedReportDates = [...allReportDates].sort((a, b) => (a > b ? 1 : a < b ? -1 : 0));

  // console.log({ filingReportDates });
  console.log({ sortedReportDates });

  const datesWriter = _createCsvWriter(datesFile, logger);

  datesWriter.push(sortedReportDates.map((d) => ({ date: d })));
  await datesWriter.flush();

  // 3. Load ALL holdings
  const holdingsFiles = await loadDataset(holdingsDir, options);

  // 4. Populate 'report_date' on holdings by matching `filingExternalId` to `externalId`
  // 5. Group holdings by `fundCik` (fund) AND `cusip` (comp). This way we should get all
  //    investements (holdings) in a single company, made by a single fund, across all years.
  const groupedHoldings: Record<string, string> = {};
  for (const filename of holdingsFiles) {
    const file = path.join(holdingsDir, filename);
    await loadDatasetCsv(holdingsDir, file, {
      ...parseConfig,
      ignoreErrors: true,
      onRow: (row) => {
        const data = {
          ...pick(row, 'fundCik', 'cusip', 'value', 'sharesOrPrincipalAmount'),
          reportDate: filingReportDates[row.filingExternalId],
        };

        const groupId = `${row.fundCik}__${row.cusip}`;
        const filepath = groupedHoldings[groupId] ?? path.join(tmpHoldingsDir, groupId);

        const tmpWriter = _createCsvWriter(filepath, logger);

        if (groupedHoldings[groupId]) {
          // Set headers so the writer doesn't try to add headers to the file
          // if it has been already set
          tmpWriter.headers.push(...Object.keys(data));
        }

        tmpWriter.push([data]);
        tmpWriter.flushSync();

        if (!groupedHoldings[groupId]) {
          groupedHoldings[groupId] = filepath;
        }
      },
    });
  }

  const deltaWriter = _createCsvWriter(outFile, logger);

  // Easiest here would be to just assume that fund holds a company from a start
  // to end, and just set the value delta depending on the previous value.
  // BUT, it can be that they get a company, and then they sell it for some period,
  // and then they get it back.
  //
  // So instead, we should check whether the date difference between the current and
  // last report is ~3 months (e.g. cutoff at 4mo). And if it's more, than we assume
  // that there was a gap, in which the company was sold. So taht means if it's
  // 1, 2, _, _, 1,
  // then we should treat it as:
  // 1, 2, 0, 0, 1.
  //
  // But the problem here is that if we look ONLY at the held companies, we don't know
  // all the report dates at which the holdings were 0.
  //
  // So what we should do is:
  // 1. Parse the filings data to get ALL values for report dates, and sort them.
  // 2. For each fund-company pair, cache entries by report date.
  // 3. Go report_date one by one, from oldest to newest,
  //    - For the very first report_date, set the delta to 0
  //    - For every subsequent, look if there is corresponding entry.
  //      - if NOT found and PREV EXISTS, then the holdings were sold, so set delta to -1 * (prev_value)
  //      - if NOT found and NOT PREV EXIST, then it's been sold earlier, so set to 0
  //      - if FOUND, and set delta to 1 * (prev_value)
  // 4. Thus, we produce a new dataset, with fields `fundCik, `cusip`, `report_date`, `delta`
  let index = 0;
  const groupedHoldingsFiles = await loadDataset(tmpHoldingsDir, { verbose: true });
  for (const groupedHoldingsFilename of groupedHoldingsFiles) {
    const filepath = path.join(tmpHoldingsDir, groupedHoldingsFilename);

    const fundCompReportsByYear: Record<
      string,
      Record<'fundCik' | 'cusip' | 'value' | 'sharesOrPrincipalAmount' | 'reportDate', any>
    > = {};
    await loadDatasetCsv(tmpHoldingsDir, filepath, {
      verbose: false,
      onRow: (data, index) => {
        fundCompReportsByYear[data.reportDate] = data;
      },
    });

    // NOTE: These should be identical for ALL entries, so just take it from first one
    const fundCik = Object.values(fundCompReportsByYear)[0].fundCik;
    const cusip = Object.values(fundCompReportsByYear)[0].cusip;

    const deltaData: {
      fundCik: string;
      cusip: string;
      report_date: string;
      value: number;
      valueDelta: number;
      sharesOrPrincipalAmount: number;
      sharesOrPrincipalAmountDelta: number;
    }[] = [];
    let prevEntry: (typeof deltaData)[number] | null = null;
    sortedReportDates.forEach((reportDate) => {
      const entry = {
        fundCik,
        cusip,
        report_date: reportDate,
        value: 0,
        valueDelta: 0,
        sharesOrPrincipalAmount: 0,
        sharesOrPrincipalAmountDelta: 0,
      };

      // Try to get corresponding entry
      const report = fundCompReportsByYear[reportDate];

      if (report) {
        if (!prevEntry) {
          // First entry that has no previous report to compare against.
          // In this case we keep delta at 0 so it doesn't falsely suggest that there
          // was a huge gain here.
          entry.value = report.value;
          entry.valueDelta = 0;
          entry.sharesOrPrincipalAmount = report.sharesOrPrincipalAmount;
          entry.sharesOrPrincipalAmountDelta = 0;
        } else {
          // Subsequent entries
          const newValue = report.value;
          const oldValue = prevEntry.value;
          entry.value = newValue;
          entry.valueDelta = newValue - oldValue;

          const newSharesAmount = report.sharesOrPrincipalAmount;
          const oldSharesAmount = prevEntry.sharesOrPrincipalAmount;
          entry.sharesOrPrincipalAmount = newSharesAmount;
          entry.sharesOrPrincipalAmountDelta = newSharesAmount - oldSharesAmount;
        }
      }
      // Cases for when there is NO report for given date for given fund+company combo
      else {
        if (!prevEntry) {
          // First entry that has no previous report to compare against. No value becase
          // no report.
          entry.value = 0;
          entry.valueDelta = 0;
          entry.sharesOrPrincipalAmount = 0;
          entry.sharesOrPrincipalAmountDelta = 0;
        } else {
          // If previous entry also had zero value, the equity is SOLD, so we omit
          // any further empty rows.
          if (!prevEntry.value) {
            return;
          }

          // Since previous entry exists, but current does not, that means that holdings were sold,
          // so set delta to -1 * (prev_value)
          entry.value = 0;
          entry.valueDelta = -1 * prevEntry.value;
          entry.sharesOrPrincipalAmount = 0;
          entry.sharesOrPrincipalAmountDelta = -1 * prevEntry.sharesOrPrincipalAmount;
        }
      }

      deltaData.push(entry);
      prevEntry = entry;
    });

    deltaWriter.push(deltaData);

    if (index && index % 1000 == 0) await deltaWriter.flush();

    index++;
  }

  await deltaWriter.flush();

  await fsp.rm(tmpHoldingsDir, { recursive: true, force: true });
};

const postprocessSec13fDataset = async () => {
  console.log({ datasetDirs });
  // const dirs = datasetDirs.slice(0, 1 /** #TODO */);
  const dirs = datasetDirs;

  // TODO UNCOMMENT
  // // 1. Fix and prepare data format
  // for (const datasetDir of dirs) {
  //   await mergePartialFiles(datasetDir);
  //   await mergeAmendmentFiles(datasetDir, { verbose: true, ignoreJsonError: true });
  // }

  // NOTE: DELETE? I DON'T THINK IT WORKED WELL
  // const lookups = await prepareCikCusip({ verbose: true });
  // for (const datasetDir of dirs) {
  //   await assocCikAndCusip(datasetDir, lookups, { verbose: true, ignoreJsonError: true });
  // }

  // NOTE: DELETE? I DON'T THINK IT WORKED WELL
  // const cikToCompanyNames = await prepareCompanyNames({ verbose: true });
  // for (const datasetDir of dirs) {
  //   await addCompanyNames(datasetDir, cikToCompanyNames, { verbose: true, ignoreJsonError: true });
  // }

  // TODO UNCOMMENT
  // // 2. Generate Funds, Equities, and Holdings dataset
  // for (const datasetDir of dirs) {
  //   await extractFunds(datasetDir, { verbose: true, ignoreJsonError: true });
  //   await extractFiling(datasetDir, { verbose: true, ignoreJsonError: true });
  //   await extractHoldings(datasetDir, { verbose: true, ignoreJsonError: true });
  // }

  // TODO - Go over holdings.csv, and aggregate the data by:
  // LVL 1 - By filing (so it's grouped by quarters AND funds )
  // LVL 2 -

  // Questions:
  // - Given a specific period and fund, show percentages of shares per each holding
  //   - ANSWER: Get all holding for specific filing, sum up all shares (counting only shares,
  //             NOT principals (bonds)), and then add field for percentage for each holding.
  // - Given a specific period and equities, show all funds invested at that point + percentages of
  //   shares per each holding

  await extractValueDeltaFromFilings({ verbose: true, ignoreErrors: true }).catch(console.error);
};

// TODO
postprocessSec13fDataset().catch(console.error);

// TODO - So I have only sec13f-2014 to sec13f-2017 datasets in `data/sec13f/`
// 1. Download other years (2018-2024)
// 2. Run whole pipeline
