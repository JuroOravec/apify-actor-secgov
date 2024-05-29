// import { type ApifyReadmeTemplatesOverrides, renderApifyReadme } from 'crawlee-one';

// import actorSpec from './actorspec';

// const templates = {
//   input: {
//     maxCount: 'outputMaxEntries',
//     privacyName: 'Include personal data',
//   },

//   perfTables: {
//     // TODO
//     default: {
//       rows: [
//         // { rowId: 'fast', template: 'Fast run' },
//         // { rowId: 'detailed', template: 'Detailed run' },
//       ],
//       cols: [
//         // { colId: '100items', template: '100 results' },
//         // { colId: 'fullRun', template: 'Full run (~ <%~ it.fn.millify(it.dataset.size) %> results)' }, // prettier-ignore
//       ],
//     },
//   },

//   features: {
//     // TODO
//     // noBrowser: {
//     //   beforeEnd: `- High throughput - browser paginates at 10 entries per page. This scraper goes at 500 entries per page
//     //   (~ 13x speed performance improvement).`,
//     // },
//   },

//   exampleInputs: [
//     // TODO
//     // {
//     //   title: `Get the first 200 organisations (fast mode)`,
//     //   inputData: {
//     //     datasetType: 'organisations',
//     //     entryIncludeLinkedResources: false,
//     //     listingFilterMaxCount: 200,
//     //     listingItemsPerPage: 200,
//     //   },
//     //   inputDataComments: {
//     //     entryIncludeLinkedResources: 'Omit relationships to other entries',
//     //   },
//     // },
//     // {
//     //   title: `Same as above, but specified by providing a start URL`,
//     //   inputData: {
//     //     startUrls: ['https://www.skcris.sk/portal/web/guest/register-organizations'],
//     //     entryIncludeLinkedResources: false,
//     //     listingFilterMaxCount: 200,
//     //     listingItemsPerPage: 200,
//     //   },
//     //   inputDataComments: {
//     //     entryIncludeLinkedResources: 'Omit relationships to other entries',
//     //   },
//     // },
//     // {
//     //   title: `Get all researchers (detailed mode)`,
//     //   inputData: {
//     //     datasetType: 'researchers',
//     //     entryIncludeLinkedResources: true,
//     //   },
//     //   inputDataComments: {
//     //     entryIncludeLinkedResources: 'Include relationships to other entries',
//     //   },
//     // },
//     // {
//     //   title: `(Advanced) Same as above, but re-configure the crawler to increase concurrency`,
//     //   inputData: {
//     //     datasetType: 'researchers',
//     //     entryIncludeLinkedResources: true,
//     //     maxConcurrency: 8,
//     //   },
//     //   inputDataComments: {
//     //     entryIncludeLinkedResources: 'Include relationships to other entries',
//     //   },
//     // },
//   ],

//   hooks: {
//     // TODO
//     //     introAfterBegin: `[SK CRIS](https://www.skcris.sk/portal/) - Slovak Current research information system
//     // ([What is CRIS?](https://en.wikipedia.org/wiki/Current_research_information_system)) - Is the authoritative database
//     // on research in Slovakia.
//     // The database contains contact info, outputs, services,
//     // and project participation of organisations and researchers registered in Slovakia.
//     // If an organisation or a researcher received public funding for their research, you can
//     // find them here.`,
//     //     introAfterDatasets: `
//     // For each of the above, you can:
//     // - Filter down by geographic region (kraj), or by starting letter
//     // - Select whether to include all related resources (e.g. org's employees, projects, equipment, ...)`,
//     //     useCases: `
//     // Lead gen:
//     // - Find potential partners or clients for your service or product.
//     // Research:
//     // - Find organisations and researchers related to your research topic of interest.
//     // - Find organisations with the equipment you need.
//     // - Find mentors, potential employers or employees
//     // Analysis:
//     // - Find the most active researchers by field, region, or overall.
//     // - Understand research and funding trends over time.`,
//     //     costAfterPerfTables: `
//     // ### Whole database
//     // - Fast run - $8.14 over 10-20h
//     // - Detailed run - $35.51 over 42-90h
//     // NOTE: Prices are only indicative, based on runs of 200 entries.`,
//     //     costBeforeEnd: `
//     // ### Comments on performance
//     // The speed of scraping depends on:
//     // 1. Detailed vs fast mode:
//     //     - In fast mode, we only request the HTML (web page). This is much faster.
//     //     - In detailed mode, we make 10-20+ more requests against the server, and
//     //     the server has to look up items in the database for each request. Hence it's slower.
//     // 2. Server load
//     //     - If there is no load on the server, we can do more requests to get the results faster.
//     //     - But if there is already considerable load (e.g. someone else is scraping too),
//     //     then expect a slower response.
//     // > **To ensure you get the data, you should configure the crawler settings to minimize timeout errors**
//     // Recommended settings:
//     // - **maxConcurrency** - Up to 8. However, if you keep getting timeour errors, the server
//     //   might be under heavier load, in which case set maxConcurrency to 3.
//     // - **Memory (RAM)** - 512 MB (there's no point in going higher), or 256 MB if maxConcurrency is set to 3.
//     // - **Timeout** - No timeout.`,
//     //     limitBeforeEnd: `Best practice:
//     // - Keep \`listingFilterMaxCount\` no higher than 500. Increasing \`listingFilterMaxCount\`
//     // increases 1) the chance of the requests failing and 2) memory requirements.
//     // - Set \`listingFilterMaxCount\` to a multiple of \`listingItemsPerPage\`. Otherwise,
//     //   you will get more results than you wanted.
//     //     Example: If I want 800 results, I set \`listingFilterMaxCount\` to 800,
//     //     and \`listingItemsPerPage\` to 400.`,
//   },
// } satisfies ApifyReadmeTemplatesOverrides;

// renderApifyReadme({ filepath: './actors/sec13f/.actor/README.md', actorSpec, templates });
