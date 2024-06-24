import Joi from 'joi';
import { allActorInputValidationFields } from 'crawlee-one';

import type { Sec13fActorInput } from './config';

const inputValidationSchema = Joi.object<Sec13fActorInput>({
  ...allActorInputValidationFields,
  secUserAgent: Joi.string().min(1).optional(),
} satisfies Record<keyof Sec13fActorInput, Joi.Schema | Joi.Schema[]>);

export const validateInput = (input: Sec13fActorInput | null) => {
  Joi.assert(input, inputValidationSchema);
};
