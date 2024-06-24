import {
  createActorConfig,
  createActorInputSchema,
  Field,
  ActorInputSchema,
  createActorOutputSchema,
  createStringField,
} from 'apify-actor-config';
import { AllActorInputs, allActorInputs as _allActorInputs } from 'crawlee-one';

import actorSpec from './actorspec';

export type Sec13fCustomActorInput = {
  secUserAgent: string;
};

/** Shape of the data passed to the actor from Apify */
export type Sec13fActorInput = Sec13fCustomActorInput & Omit<AllActorInputs, 'ignoreSslErrors'>;

const customActorInput = {
  /** No custom fields currently */
  secUserAgent: createStringField({
    title: 'SEC User Agent HTTP Header',
    type: 'string',
    editor: 'textfield',
    description:
      `Per the SEC Webmaster FAQ, you need to declare your user agent. in following format: <br/> <br/>` +
      `User-Agent: Sample Company Name AdminContact@<sample company domain>.com <br/> <br/>` +
      `See https://www.sec.gov/os/webmaster-faq#code-support`,
    minLength: 1,
    nullable: true,
  }),
} satisfies Record<keyof Sec13fCustomActorInput, Field>;

// Customize the default options

// 'ignoreSslErrors' is not applicable to Playwright
/* eslint-disable-next-line @typescript-eslint/no-unused-vars */
const { ignoreSslErrors, ...allActorInputs } = _allActorInputs;

allActorInputs.requestHandlerTimeoutSecs.prefill = 60 * 5; // 5m
allActorInputs.maxRequestRetries.default = 5;
allActorInputs.maxRequestRetries.prefill = 5;
allActorInputs.maxConcurrency.default = 1;
allActorInputs.maxConcurrency.prefill = 1;

const inputSchema = createActorInputSchema<ActorInputSchema<Record<keyof Sec13fActorInput, Field>>>(
  {
    schemaVersion: 1,
    title: actorSpec.actor.title,
    description: `Configure the ${actorSpec.actor.title}.`,
    type: 'object',
    properties: {
      ...customActorInput,
      // Include the common fields in input
      ...allActorInputs,
    },
  }
);

const outputSchema = createActorOutputSchema({
  actorSpecification: 1,
  fields: {},
  views: {},
});

const config = createActorConfig({
  actorSpecification: 1,
  name: actorSpec.platform.actorId,
  title: actorSpec.actor.title,
  description: actorSpec.actor.shortDesc,
  version: '1.0',
  dockerfile: '../Dockerfile',
  dockerContextDir: '../../..',
  input: inputSchema,
  storages: {
    dataset: outputSchema,
  },
});

export default config;
