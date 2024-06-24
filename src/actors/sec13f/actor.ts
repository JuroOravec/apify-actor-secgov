import { type CheerioCrawlerOptions, Request } from 'crawlee';

import { secf13Routes } from './router';
import { validateInput } from './validation';
import { getPackageJsonInfo } from '../../utils/package';
import { createSecClient } from '../../lib/secClient';
import { sec13fCrawler } from './__generated__/crawleeone';

export const run = async (crawlerConfig?: CheerioCrawlerOptions): Promise<void> => {
  const pkgJson = getPackageJsonInfo(module, ['name']);

  await sec13fCrawler({
    name: pkgJson.name,
    routes: secf13Routes,
    crawlerConfig,
    mergeInput: true,
    inputDefaults: {
      perfBatchWaitSecs: 1,
      maxConcurrency: 1,
      maxRequestsPerMinute: 120,
      requestHandlerTimeoutSecs: 60 * 5, // 5m
      // REQUIRED to download SEC index files like
      // https://www.sec.gov/Archives/edgar/full-index/2023/QTR1/master.idx
      additionalMimeTypes: ['application/octet-stream'],

      // TODO: Allow to specify input in this framework
      // @ts-ignore
      secUserAgent: 'Sample Company Name AdminContact@<sample company domain>.com',

      // FOR TESTING ONLY
      // maxRequestsPerCrawl: 100,
      // logLevel: 'debug',
    },
    hooks: {
      // @ts-ignore
      validateInput,
      onReady: async (actor) => {
        const secClient = createSecClient({
          // @ts-ignore
          userAgent: actor.input?.secUserAgent,
        });

        // TEST 1
        const requests = [
          new Request({
            // url: 'https://www.sec.gov/Archives/edgar/data/1003518/000094562123000384',
            url: 'https://www.sec.gov/Archives/edgar/data/1011443/000101144317000007',
          }),
        ];

        // // TEST 2
        // const periods = secClient.generatePeriods(new Date('2023-09-28')).slice(0, 1);
        // const requests = periods.map((p) => {
        //   const url = secClient.genSECIndexFileUrl(p);
        //   return new Request({ url });
        // });

        // // FULL DATASET
        // const periods = secClient.generatePeriods();
        // const requests = periods.map((p) => {
        //   const url = secClient.genSECIndexFileUrl(p);
        //   return new Request({ url });
        // });

        await actor.runCrawler(requests);
      },
    },
  }).catch((err) => {
    console.log(err);
    throw err;
  });
};
