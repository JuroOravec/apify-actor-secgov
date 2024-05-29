import { AllActorInputs, CrawleeOneActorRouterCtx, CrawleeOneActorInst, CrawleeOneRoute, CrawleeOneRouteHandler, CrawleeOneRouteWrapper, CrawleeOneRouteMatcher, CrawleeOneRouteMatcherFn, CrawleeOneIO, CrawleeOneTelemetry, CrawleeOneCtx, CrawleeOneArgs, crawleeOne } from "crawlee-one"
import type { BasicCrawlingContext, HttpCrawlingContext, CheerioCrawlingContext, JSDOMCrawlingContext, PlaywrightCrawlingContext, PuppeteerCrawlingContext } from "crawlee"


export type MaybePromise<T> = T | Promise<T>;

export type sec13fLabel = "EDGAR_INDEX_FILE" | "EDGAR_FILING_DIR" | "EDGAR_FILING_XML_FILE";

export enum sec13fLabelEnum {
  'EDGAR_INDEX_FILE' = 'EDGAR_INDEX_FILE',
  'EDGAR_FILING_DIR' = 'EDGAR_FILING_DIR',
  'EDGAR_FILING_XML_FILE' = 'EDGAR_FILING_XML_FILE'
}

export type sec13fCtx<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneCtx<CheerioCrawlingContext, sec13fLabel, TInput, TIO, Telem>;

export const sec13fCrawler = <TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>>(args: Omit<CrawleeOneArgs<"cheerio", sec13fCtx<TInput, TIO, Telem>>, 'type'>) => crawleeOne<"cheerio", sec13fCtx<TInput, TIO, Telem>>({ ...args, type: "cheerio"});;

export type sec13fRouterContext<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneActorRouterCtx<sec13fCtx<TInput, TIO, Telem>>;

export type sec13fActorCtx<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneActorInst<sec13fCtx<TInput, TIO, Telem>>;

export type sec13fRoute<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneRoute<sec13fCtx<TInput, TIO, Telem>, sec13fRouterContext<TInput, TIO, Telem>>;

export type sec13fRouteHandler<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneRouteHandler<sec13fCtx<TInput, TIO, Telem>, sec13fRouterContext<TInput, TIO, Telem>>;

export type sec13fRouteWrapper<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneRouteWrapper<sec13fCtx<TInput, TIO, Telem>, sec13fRouterContext<TInput, TIO, Telem>>;

export type sec13fRouteMatcher<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneRouteMatcher<sec13fCtx<TInput, TIO, Telem>, sec13fRouterContext<TInput, TIO, Telem>>;

export type sec13fRouteMatcherFn<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneRouteMatcherFn<sec13fCtx<TInput, TIO, Telem>, sec13fRouterContext<TInput, TIO, Telem>>;

export type sec13fOnBeforeHandler<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneRouteHandler<sec13fCtx<TInput, TIO, Telem>, sec13fRouterContext<TInput, TIO, Telem>>;

export type sec13fOnAfterHandler<TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>> = CrawleeOneRouteHandler<sec13fCtx<TInput, TIO, Telem>, sec13fRouterContext<TInput, TIO, Telem>>;

export type sec13fOnReady = <TInput extends Record<string, any> = AllActorInputs, TIO extends CrawleeOneIO = CrawleeOneIO, Telem extends CrawleeOneTelemetry<any, any> = CrawleeOneTelemetry<any, any>>(actor: sec13fActorCtx<TInput, TIO, Telem>) => MaybePromise<void>;;