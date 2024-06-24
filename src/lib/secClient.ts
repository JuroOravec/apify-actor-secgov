import axios, { type AxiosRequestConfig } from 'axios';
import { subMonths, format, parse, subDays } from 'date-fns';
import cheerio, { type Cheerio } from 'cheerio';
import xml2js from 'xml2js';

interface ThirteenFFiling {
  externalId: string;
  companyName: string;
  formType: string;
  cik: string;
  dateFiled: Date;
  fullSubmissionUrl: string;
  directoryUrl: string;
  secIndexUrl: string;
}

type ThirteenFFilingEntryCol = 'CIK' | 'Company Name' | 'Form Type' | 'Date Filed' | 'Filename';

interface Holding {
  cusip: string;
  issuerName: string;
  classTitle: string;
  value: number;
  sharesOrPrincipalAmount: string;
  sharesOrPrincipalAmountType: string;
  optionType: string;
  investmentDiscretion: string;
  otherManager: string;
  votingAuthoritySole: string;
  votingAuthorityShared: string;
  votingAuthorityNone: string;
}

interface Period {
  year: number;
  quarter: number;
}

const BASE_URL = 'https://www.sec.gov';
const THIRTEEN_F_FORM_TYPES = ['13F-HR', '13F-HR/A'];
// The first year expected to have XML urls
const XML_START_YEAR = 2014;

const paddedCik = (cik: string | number): string => {
  return cik.toString().padStart(10, '0');
};

// NOTE: We cannot rely solely on the document names to know what content it will have.
// Maybe we could in case of "primary_doc", but "info_table" XMLs often have various names.
export const isXmlPrimaryDoc = (doc: Cheerio<any>) => {
  return !!doc.find('edgarSubmission').text().trim();
};
export const isXmlInfoTable = (doc: Cheerio<any>) => {
  return !!doc.find('informationTable').text().trim();
};

const xmlRemovePrefixes = async (xml: string) => {
  // Create a parser
  const parser = new xml2js.Parser({
    tagNameProcessors: [xml2js.processors.stripPrefix],
  });

  // Parse the XML, remove prefixes and log the result
  const result = await parser.parseStringPromise(xml);

  // Converting back to string to see the output
  const builder = new xml2js.Builder({ renderOpts: { pretty: false } });
  const newXml = builder.buildObject(result);

  return newXml;
};

export const preprocessXml = async (xml: string) => {
  const data = await xmlRemovePrefixes(xml);
  return data.replace(/\s+/g, ' ');
};

export const createSecClient = (config: { baseUrl?: string; userAgent: string }) => {
  const { baseUrl = BASE_URL, userAgent } = config ?? {};

  const getUrl = (url: string, options?: AxiosRequestConfig) => {
    const headers = {
      ...(options?.headers ?? {}),
      // NOTE: Per the SEC Webmaster FAQ, you need to declare your user agent.
      // in following format:
      //
      // `User-Agent: Sample Company Name AdminContact@<sample company domain>.com`
      //
      // See https://www.sec.gov/os/webmaster-faq#code-support
      'User-Agent': userAgent,
    };

    return axios.get(url, { ...options, headers });
  };

  /**
   * Make SEC index file URL such as the one at
   * https://www.sec.gov/Archives/edgar/full-index/2024/QTR1/master.idx
   */
  const genSECIndexFileUrl = (period: Period): string => {
    const url = `${baseUrl}/Archives/edgar/full-index/${period.year}/QTR${period.quarter}/master.idx`;
    return url;
  };

  /**
   * Parse SEC index files such as the one at
   * https://www.sec.gov/Archives/edgar/full-index/2024/QTR1/master.idx
   *
   * These files are like CSV with extra metadata at top:
   *
   * ```txt
   * Description:           Master Index of EDGAR Dissemination Feed
   * Last Data Received:    March 31, 2024
   * Comments:              webmaster@sec.gov
   * Anonymous FTP:         ftp://ftp.sec.gov/edgar/
   * Cloud HTTP:            https://www.sec.gov/Archives/
   *
   * CIK|Company Name|Form Type|Date Filed|Filename
   * --------------------------------------------------------------------------------
   * 1000045|NICHOLAS FINANCIAL INC|10-Q|2024-02-13|edgar/data/1000045/0000950170-24-014566.txt
   * 1000045|NICHOLAS FINANCIAL INC|424B3|2024-03-19|edgar/data/1000045/0000950170-24-033226.txt
   * ```
   */
  const parseSECIndexFile = <TDataKeys extends string = string, TMetaKeys extends string = string>(
    content: string,
    options?: { delimeter?: string }
  ): {
    entries: Record<TDataKeys, string>[];
    meta: Record<TMetaKeys, string>;
  } => {
    const { delimeter = '|' } = options ?? {};

    const meta = {} as Record<TMetaKeys, string>;
    const entries: Record<TDataKeys, string>[] = [];
    let colNames: string[] = [];
    let isParsingMeta = true;

    content.split('\n').forEach((rawLine) => {
      const line = rawLine.replace(/[^\x20-\x7E]+/g, '').trim();
      const isEmpty = !line;

      if (isEmpty) {
        // We've reached the end of metadata
        if (isParsingMeta) isParsingMeta = false;

        return;
      }

      // Parse file metadata
      if (isParsingMeta) {
        const [rawKey, ...values] = line.split(':', 1);
        const key = rawKey.trim() as TMetaKeys;
        const value = values.join(':').trim();
        meta[key] = value;
        return;
      }

      // Parse file data
      const parts = line.split(delimeter);

      // Between the data headers and data entries, there's a dividing line. Ignore it.
      if (!parts.length) return;

      // We've come across the header row
      if (!colNames.length) {
        colNames = parts;
        return;
      }

      // If we got all the way here, we're parsing a data line
      const row = colNames.reduce<Record<TDataKeys, string>>(
        (agg, colName, index) => ({ ...agg, [colName]: parts[index] }),
        {} as any
      );

      entries.push(row);
    });

    return { meta, entries };
  };

  const get13FFilings = async ({ period }: { period: Period }): Promise<ThirteenFFiling[]> => {
    const url = genSECIndexFileUrl(period);
    const response = await getUrl(url);
    if (!response.data) throw Error('Invalid response for get13FFilingsSinceDate');

    return parse13FFilings(response.data);
  };

  const parse13FFilings = async (content: string): Promise<ThirteenFFiling[]> => {
    const { entries } = parseSECIndexFile<ThirteenFFilingEntryCol>(content);

    const thirteenFs = entries.reduce<ThirteenFFiling[]>((agg, entry) => {
      const cik = entry['CIK'];
      const formType = entry['Form Type'];
      const filename = entry['Filename'];
      const companyName = entry['Company Name'];
      const dateFiled = entry['Date Filed'];

      // TODO - Also scrape other filing types?
      if (!THIRTEEN_F_FORM_TYPES.includes(formType)) return agg;

      const fullSubmissionUrl = `${baseUrl}/Archives/${filename}`;
      const directoryUrl = fullSubmissionUrl.replace('.txt', '').replace(/-/g, '');
      const externalId = directoryUrl.split('/').pop()!;

      agg.push({
        externalId,
        companyName,
        formType,
        cik: paddedCik(cik),
        dateFiled: parse(dateFiled, 'yyyy-MM-dd', new Date()),
        fullSubmissionUrl,
        directoryUrl,
        secIndexUrl: secIndexUrl(externalId, directoryUrl),
      });

      return agg;
    }, []);

    return thirteenFs;
  };

  const get13FFilingsByPeriods = async (input?: { periods?: Period[] }) => {
    const periods = input?.periods || getPeriodsForLastYear();

    const startTime = new Date();
    console.log(
      `${format(
        startTime,
        'yyyy-MM-dd HH:mm:ss'
      )}: beginning minimal db seed, might take a few minutes…`
    );

    const filings: ThirteenFFiling[] = [];

    for (const p of periods) {
      console.log(
        `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')}: importing 13Fs filed in ${p.year} Q${
          p.quarter
        }`
      );
      const newFilings = await get13FFilings({ period: p });
      filings.push(...newFilings);
      console.log(
        `${format(new Date(), 'yyyy-MM-dd HH:mm:ss')}: DONE importing 13Fs filed in ${p.year} Q${
          p.quarter
        }`
      );
    }

    return filings;
  };

  const generatePeriods = (from?: Date, to?: Date): Period[] => {
    const toDate = to || new Date();
    const toYear = toDate.getFullYear();
    const toQuarter = Math.floor(toDate.getMonth() / 3) + 1;

    const fromDate = from || new Date(`${XML_START_YEAR}`);
    const fromYear = fromDate.getFullYear();

    const yearsRange = Array.from({ length: toYear - fromYear + 1 }, (_, i) => fromYear + i);
    const quarters = [1, 2, 3, 4];

    const allPeriods = yearsRange
      .flatMap((year) => quarters.map((quarter) => [year, quarter]))
      .map(([year, quarter]): Period => ({ year, quarter }))
      .filter((p) => !(p.year === toYear && p.quarter > toQuarter));

    return allPeriods;
  };

  const get13FFilingsAll = async (): Promise<ThirteenFFiling[]> => {
    const periods = generatePeriods();

    const allFilings: ThirteenFFiling[] = [];
    for (const period of periods) {
      console.log(
        `${new Date().toISOString()}: importing 13F forms filed during ${period.year} Q${
          period.quarter
        }`
      );
      const newFilings = await get13FFilings({ period });
      allFilings.push(...newFilings);
    }

    console.log(
      `${new Date().toISOString()}: all 13F forms imported, but you'll need to process them to fetch all of the holdings data`
    );

    return allFilings;
  };

  const get13FFilingsSinceDate = async (
    input: {
      filedSince?: Date;
      perPage?: number;
      maxPages?: number;
    } = {}
  ): Promise<ThirteenFFiling[]> => {
    const { filedSince = subDays(new Date(), 1), perPage = 100, maxPages = 100 } = input;

    const url = `${baseUrl}/cgi-bin/browse-edgar`;
    const queryParams = {
      action: 'getcurrent',
      count: perPage.toString(),
      output: 'atom',
      type: '13F-HR',
    };

    let offset = 0;

    // 13F-HR/A - Allianz Asset Management GmbH (0001535323)
    const titleRegex = /^13F\-HR(?:\/A)? - (.+?) \((\d{10})\)/;
    const results: ThirteenFFiling[] = [];

    for (let i = 0; i < maxPages; i++) {
      const params = { ...queryParams, start: offset };
      const response = await getUrl(url, { params });
      if (!response.data) throw Error('Invalid response for get13FFilingsSinceDate');

      // Parse XML like below:
      // ```
      // <feed xmlns="http://www.w3.org/2005/Atom">
      //   <entry>
      //     <title>13F-HR/A - WFA Asset Management Corp (0001994434) (Filer)</title>
      //     <link rel="alternate" type="text/html" href="https://www.sec.gov/Archives/edgar/data/1994434/000199443424000004/0001994434-24-000004-index.htm"/>
      //     <summary type="html">
      //       &lt;b&gt;Filed:&lt;/b&gt; 2024-05-24 &lt;b&gt;AccNo:&lt;/b&gt; 0001994434-24-000004 &lt;b&gt;Size:&lt;/b&gt; 122 KB
      //     </summary>
      //     <updated>2024-05-24T17:22:18-04:00</updated>
      //     <category scheme="https://www.sec.gov/" label="form type" term="13F-HR/A"/>
      //     <id>urn:tag:sec.gov,2008:accession-number=0001994434-24-000004</id>
      //   </entry>
      //   ...
      // </feed>
      // ```
      const dom = cheerio.load(response.data);
      const root = dom.root();

      const entries = root.find('feed > entry').toArray() || [];

      entries.forEach((entry) => {
        const theEl = dom(entry);

        // Already in ISO format '2024-05-24T17:41:21-04:00'
        const dateFileStr = theEl.find('updated').first().text();
        const dateFiled = new Date(dateFileStr);

        if (dateFiled < filedSince) return;

        const submissionUrl = theEl.find('link').first().attr('href');
        // Turn https://www.sec.gov/Archives/edgar/data/1994434/000199443424000004/0001994434-24-000004-index.htm
        // into https://www.sec.gov/Archives/edgar/data/1994434/000199443424000004
        const directoryUrl = submissionUrl?.split('/').slice(0, -1).join('/');
        if (!directoryUrl) throw Error('Failed to find directory URL for entry');

        // Extract 000199443424000004
        // from https://www.sec.gov/Archives/edgar/data/1994434/000199443424000004
        const submissionId = directoryUrl.split('/').slice(-1)[0];
        // Format `000199443424000004` as `0001994434-24-000004`
        const submissionIdFormatted = `${submissionId.slice(0, 10)}-${submissionId.slice(10, 12)}-${submissionId.slice(12)}`; // prettier-ignore
        // Turn https://www.sec.gov/Archives/edgar/data/1994434/000199443424000004
        // into https://www.sec.gov/Archives/edgar/data/1994434/0001994434-24-000004.txt
        const submissionUrlRoot = directoryUrl.split('/').slice(0, -1).join('/');
        const fullSubmissionUrl = `${submissionUrlRoot}/${submissionIdFormatted}.txt`;

        const externalId = directoryUrl.split('/').pop();
        if (!externalId) throw Error('Failed to find externalId');

        const formType = theEl
          .find('category')
          .toArray()
          .map((el) => dom(el))
          .find((el) => el.attr('label') === 'form type')
          ?.attr('term');
        if (!formType) throw Error('Failed to find formType');

        results.push({
          externalId,
          companyName: (theEl.find('title').first().text().match(titleRegex) || [])[1],
          formType,
          cik: paddedCik(directoryUrl.split('/').slice(-2)[0]),
          dateFiled,
          directoryUrl,
          fullSubmissionUrl,
          secIndexUrl: secIndexUrl(externalId, directoryUrl),
        });
      });

      if (entries.length < perPage) break;

      offset = offset + perPage;
    }

    return results;
  };

  const parsePrimaryDocXml = async (xml: string) => {
    try {
      const dom = cheerio.load(xml);
      const root = dom.root();
      const dateString = root.find('reportCalendarOrQuarter').text().trim();
      const reportDate = parse(dateString, 'MM-dd-yyyy', new Date());

      // TODO  Check if this works?
      // TODO: Have a look at <otherManagersInfo> of
      //       https://www.sec.gov/Archives/edgar/data/908758/0001140361-17-021232.txt
      const other_managers = root
        .find('otherManagers2Info otherManager2')
        .toArray()
        .map((el) => {
          const theEl = dom(el);
          return {
            sequence_number: parseInt(theEl.find('sequenceNumber').text().trim() || '0'),
            file_number: theEl.find('form13FFileNumber').first().text().trim(),
            name: theEl.find('name').text().trim(),
          };
        });

      // TODO - Transform to camelCase
      // prettier-ignore
      return {
        report_date: reportDate,
        street1: root.find('address street1,com\\:street1').text().trim().toLowerCase(),
        street2: root.find('address street2,com\\:street2').text().trim().toLowerCase(),
        city: root.find('address city,com\\:city').text().trim().toLowerCase(),
        state_or_country: root.find('address stateOrCountry,com\\:stateOrCountry').text().trim().toUpperCase(),
        zip_code: root.find('address zipCode,com\\:zipCode').text().trim(),
        other_included_managers_count: parseInt(
          root.find('otherIncludedManagersCount').text().trim() || '0'
        ),
        holdings_count_reported: parseInt(root.find('tableEntryTotal').text().trim() || '0'),
        holdings_value_reported: parseFloat(root.find('tableValueTotal').text().trim() || '0'),
        confidential_omitted: root.find('isConfidentialOmitted').text().trim().toLowerCase() === 'true',
        report_type: root.find('reportType').text().trim().toLowerCase(),
        amendment_type: root.find('amendmentType').text().trim().toLowerCase(),
        amendment_number: parseInt(root.find('amendmentNo').text().trim() || '0'),
        file_number: root.find('coverPage form13FFileNumber').text().trim(),
        other_managers,
      };
    } catch (error) {
      console.error('Failed to parse XML', error);
      throw error;
    }
  };

  const extractHoldingsFromInfoTableXml = async (xml: string) => {
    const dom = cheerio.load(xml);
    const root = dom.root();

    // Example: https://www.sec.gov/Archives/edgar/data/1003518/000094562123000384/informationtable.xml
    const infoTables = root
      .find('infoTable')
      .toArray()
      .map((el) => dom(el));
    return infoTables.map(
      (el): Holding => ({
        cusip: el.find('cusip').text().trim().toUpperCase().padStart(9, '0'),
        issuerName: el.find('nameOfIssuer').text().trim(),
        classTitle: el.find('titleOfClass').text().trim(),
        value: parseFloat(el.find('value').text().trim() || '0'),
        sharesOrPrincipalAmount: el.find('sshPrnamt').text().trim(),
        sharesOrPrincipalAmountType: el.find('sshPrnamtType').text().trim().toLowerCase(),
        optionType: el.find('putCall').text().trim().toLowerCase(),
        investmentDiscretion: el.find('investmentDiscretion').text().trim().toLowerCase(),
        otherManager: el.find('otherManager').text().trim(),
        votingAuthoritySole: el.find('votingAuthority Sole').text().trim(),
        votingAuthorityShared: el.find('votingAuthority Shared').text().trim(),
        votingAuthorityNone: el.find('votingAuthority None').text().trim(),
      })
    );
  };

  const secIndexUrl = (externalId: string, directoryUrl: string): string => {
    const finalPath = [
      externalId.slice(0, 10),
      externalId.slice(10, 12),
      externalId.slice(12),
    ].join('-');
    const fullSubmissionUrl = `${directoryUrl.replace(/\/$/, '')}/${finalPath}.txt`;
    return fullSubmissionUrl.replace(/\.txt$/, '-index.html');
  };

  const parse13FFilingDataUrlsFromDirPage = async (content: string) => {
    const $ = cheerio.load(content);
    const root = $.root();
    const xmlUrls = root
      .find('a')
      .toArray()
      .map((a) => {
        const href = $(a).attr('href');
        return `${baseUrl}${href}`;
      })
      .filter((href) => href.toLowerCase().endsWith('.xml'));

    if (xmlUrls.length === 0) throw new Error('No XML URLs found');

    return xmlUrls;
  };

  // TODO?
  // Implement other necessary processing
  // Example: this.markPreviousFilingsAsRestated();
  // Example: this.createHoldings();

  const getPeriodsForLastYear = () => {
    const now = new Date();
    return Array.from({ length: 4 }, (_, i) => {
      const date = subMonths(now, 3 * i);
      return {
        year: date.getFullYear(),
        quarter: Math.floor(date.getMonth() / 3) + 1,
      };
    });
  };

  return {
    get13FFilings,
    get13FFilingsAll,
    get13FFilingsByPeriods,
    get13FFilingsSinceDate,
    generatePeriods,
    genSECIndexFileUrl,
    parse13FFilings,
    parse13FFilingDataUrlsFromDirPage,
    parsePrimaryDocXml,
    extractHoldingsFromInfoTableXml,
  };
};
