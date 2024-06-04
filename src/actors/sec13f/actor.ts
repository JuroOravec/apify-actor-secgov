import { type CheerioCrawlerOptions, Request } from 'crawlee';

import { secf13Routes } from './router';
import { validateInput } from './validation';
import { getPackageJsonInfo } from '../../utils/package';
import { secClient } from '../../lib/secClient';
import { sec13fCrawler } from './__generated__/crawleeone';

const YEAR = parseInt(process.env.SEC_YEAR || '2024');

export const run = async (crawlerConfig?: CheerioCrawlerOptions): Promise<void> => {
  const pkgJson = getPackageJsonInfo(module, ['name']);

  await sec13fCrawler({
    name: pkgJson.name,
    routes: secf13Routes,
    crawlerConfig,
    mergeInput: true,
    inputDefaults: {
      perfBatchWaitSecs: 1,
      maxConcurrency: 2, // NOTE: One crawler reaches about 180 reqs/min or 3 reqs/sec
      maxRequestsPerMinute: 60 * 4, // 4 per second / NOTE: We get HTTP error 429 at 5 reqs/sec
      requestHandlerTimeoutSecs: 60 * 5, // 5m
      // REQUIRED to download SEC index files like
      // https://www.sec.gov/Archives/edgar/full-index/2023/QTR1/master.idx
      additionalMimeTypes: ['application/octet-stream'],
      
      // FOR TESTING ONLY
      // maxRequestsPerCrawl: 100,
      // logLevel: 'debug',
      outputDatasetId: `secf13-${YEAR}`,
    },
    hooks: {
      validateInput,
      onReady: async (actor) => {
        // TEST 1
        // const requests = [
        //   new Request({
        //     url: 'https://www.sec.gov/Archives/edgar/data/1388391/000117266114001819',
        //   }),
        // ];

        // TEST 2
        // const periods = secClient.generatePeriods(new Date('2023-09-28')).slice(0, 1);
        // const requests = periods.map((p) => {
        //   const url = secClient.genSECIndexFileUrl(p);
        //   return new Request({ url });
        // });

        // SINGLE YEAR DATASET
        const periods = secClient.generatePeriods().filter((p) => p.year === YEAR);
        console.log({ periods })
        const requests = periods.map((p) => {
          const url = secClient.genSECIndexFileUrl(p);
          return new Request({ url });
        });

        // // FULL DATASET
        // const periods = secClient.generatePeriods();
        // console.log({ periods })
        // const requests = periods.map((p) => {
        //   const url = secClient.genSECIndexFileUrl(p);
        //   return new Request({ url });
        // });

        await actor.runCrawler(requests);
      },
    },
  }).catch((err) => {
    console.log({err});
    throw err;
  });
};
