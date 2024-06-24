import type { CheerioCrawlingContext } from 'crawlee';
import { cheerioPortadom } from 'portadom';
import { Actor } from 'apify';
import cheerio from 'cheerio';

import { sec13fLabel, sec13fRoute } from './__generated__/crawleeone';
import {
  isXmlInfoTable,
  isXmlPrimaryDoc,
  createSecClient,
  preprocessXml,
} from '../../lib/secClient';

interface UserData {
  filing: Record<string, any>;
  xmlUrls: {
    url: string;
    type: 'primary_doc' | 'info_table' | 'unknown';
    // NOTE: We include the XML, so we don't have to scrape SEC again if we need to just
    // extract some extra fields from the doc.
    // TODO - store the scraped content automatically, or with a flip of a switch?
    //      - it should have two options - whether to include it or not (bool),
    //        and an optional (maybe async) transformation function
    content: string;
  }[];
  remainingXmlUrls: string[];
}

// NOTE: Complex regexes are defined in top-level scope, so any syntax errors within them
//       are captured during initialization.
// prettier-ignore
const URL_REGEX = {
  // E.g. https://www.sec.gov/Archives/edgar/full-index/2023/QTR4/master.idx
  EDGAR_INDEX_FILE: /^\/archives\/edgar\/full-index\/(?<year>[0-9]+)\/qtr(?<qtr>[1-4])\/master\.idx/i,

  // E.g. https://www.sec.gov/Archives/edgar/data/1994434/000199443424000004
  EDGAR_FILING_DIR: /^\/archives\/edgar\/data\/(?<cik>[0-9]+)\/(?<submission>[0-9]+)$/i,

  // E.g. https://www.sec.gov/Archives/edgar/data/1000097/000100009721000004/primary_doc.xml
  // or https://www.sec.gov/Archives/edgar/data/1003518/000094562123000384/informationtable.xml
  EDGAR_FILING_XML_FILE: /^\/archives\/edgar\/data\/(?<cik>[0-9]+)\/(?<submission>[0-9]+)\/[^\/]+\.xml$/i,
};

const makeCheerioDom = async (ctx: CheerioCrawlingContext, url: string | null) => {
  const cheerioDom = await ctx.parseWithCheerio();
  const dom = cheerioPortadom(cheerioDom.root(), url);
  return dom;
};

/* eslint-disable-next-line @typescript-eslint/no-unused-vars */
export const secf13Routes = {
  // 1. Extract Filings from the index file
  EDGAR_INDEX_FILE: {
    match: (url) => {
      const urlObj = new URL(url);
      return !!urlObj.pathname.match(URL_REGEX.EDGAR_INDEX_FILE);
    },
    handler: async (ctx) => {
      const secClient = createSecClient({
        // @ts-ignore
        userAgent: ctx.actor.input.secUserAgent,
      });

      const filigns = await secClient.parse13FFilings(ctx.body.toString());

      const requests = filigns.map((filing) => ({
        url: filing.directoryUrl,
        userData: { filing } satisfies Pick<UserData, 'filing'>,
      }));

      ctx.log.debug(`Opening default request queue`);
      const reqQueue = await Actor.openRequestQueue();

      ctx.log.info(`Redirecting to filing directory pages`);
      await reqQueue.addRequests(requests);
    },
  },

  // 2. Given a "directory" page of a filing, get URLs with data
  //    and process them one-by-one if found
  EDGAR_FILING_DIR: {
    match: (url) => {
      const urlObj = new URL(url);
      return !!urlObj.pathname.match(URL_REGEX.EDGAR_FILING_DIR);
    },
    handler: async (ctx) => {
      const secClient = createSecClient({
        // @ts-ignore
        userAgent: ctx.actor.input.secUserAgent,
      });

      const xmlUrls = await secClient.parse13FFilingDataUrlsFromDirPage(ctx.body as string);

      const { filing } = ctx.request.userData as Pick<UserData, 'filing'>;

      // There's no more XML files for us to process, so save the data and leave early
      if (!xmlUrls.length) {
        const entry = { ...filing, xmlUrls };
        ctx.actor.pushData(entry, {
          // TODO?
          privacyMask: {},
        });
        return;
      }

      // Otherwise, we want to process these two URLs one by one, so we pass all the data
      // along with the next request.
      const [nextUrl, ...remainingXmlUrls] = xmlUrls;

      ctx.log.info(`Redirecting to first XML data file`);
      await ctx.actor.pushRequests(
        {
          url: nextUrl!,
          userData: {
            filing,
            xmlUrls: [],
            remainingXmlUrls,
          } satisfies UserData,
        },
        { queueOptions: { forefront: true } }
      );
    },
  },

  // 3. Extract data from an XML file, depending on which one it is. And then move
  //    onto the next one.
  EDGAR_FILING_XML_FILE: {
    match: (url) => {
      const urlObj = new URL(url);
      return !!urlObj.pathname.match(URL_REGEX.EDGAR_FILING_XML_FILE);
    },
    handler: async (ctx) => {
      const secClient = createSecClient({
        // @ts-ignore
        userAgent: ctx.actor.input.secUserAgent,
      });

      // NOTE: Not great that we parse the XML 3 times (1st by Crawlee, 2nd to strip XML prefixes,
      // and 3rd to do the *actual* scraping. But good enough for now.
      const origXml = (await ctx.parseWithCheerio()).xml();
      const xml = await preprocessXml(origXml);
      const dom = cheerio.load(xml, {
        // Options based on Crawlee's internals
        xmlMode: true,
        // @ts-expect-error // Internal
        _useHtmlParser2: true,
      });
      const rootEl = dom.root();

      const { filing, xmlUrls, remainingXmlUrls } = ctx.request.userData as UserData;

      let dataToAdd: Record<string, any> = {};
      if (isXmlPrimaryDoc(rootEl)) {
        xmlUrls.push({ url: ctx.request.url, type: 'primary_doc', content: xml });
        const primaryDocData = await secClient.parsePrimaryDocXml(xml);
        dataToAdd = { ...dataToAdd, ...primaryDocData };
      } else if (isXmlInfoTable(rootEl)) {
        xmlUrls.push({ url: ctx.request.url, type: 'info_table', content: xml });
        dataToAdd.holdings = await secClient.extractHoldingsFromInfoTableXml(xml);
      } else {
        xmlUrls.push({ url: ctx.request.url, type: 'unknown', content: xml });
      }

      // There's no more data for us to process, so save the data and leave early
      if (!remainingXmlUrls.length) {
        const entry = { ...filing, ...dataToAdd, xmlUrls };
        ctx.actor.pushData(entry, {
          // TODO?
          privacyMask: {},
        });
        return;
      }

      const [nextUrl, ...newRemainingXmlUrls] = remainingXmlUrls;

      // Otherwise, we want to process the next XML URL, so we pass all the data
      // along with the next request.
      ctx.log.info(`Redirecting to next XML data file`);
      await ctx.actor.pushRequests(
        {
          url: nextUrl,
          userData: {
            filing: { ...filing, ...dataToAdd },
            xmlUrls,
            remainingXmlUrls: newRemainingXmlUrls,
          } satisfies UserData,
        },
        { queueOptions: { forefront: true } }
      );
    },
  },
} satisfies Record<sec13fLabel, sec13fRoute>;
