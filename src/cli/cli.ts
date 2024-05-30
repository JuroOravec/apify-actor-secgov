import { spawn, exec } from 'child_process';
import { program } from 'commander';
import path from 'path';
import fsp from 'fs/promises';

import { getPackageJsonInfo } from '../utils/package';

const pkgJson = getPackageJsonInfo(module, ['name', 'version']);
const ACTOR_DIST_DIR = './dist/cjs/actors';
const ACTOR_OUT_DIR = './actors';
const ACTOR_SRC_DIR = './src/actors';

const genExampleCall = (cmdArgs: string) => {
  return `\n\nExample call:
  $ ${pkgJson.name} ${cmdArgs}
  \nUse two double dashes to pass extra arguments to the command:
  $ ${pkgJson.name} ${cmdArgs} -- -- argVal -o ./out`;
};

const getAllActors = async () => {
  const fileNames = await fsp.readdir(ACTOR_DIST_DIR);
  const actorNames = await fileNames.reduce<Promise<string[]>>(async (aggPromise, dirName) => {
    const agg = await aggPromise;
    const fullPath = path.join(ACTOR_DIST_DIR, dirName);
    const isDir = (await fsp.stat(fullPath)).isDirectory();
    if (isDir) agg.push(dirName);
    return agg;
  }, Promise.resolve([]));
  return actorNames;
};

const execCmd = (cmd: string) => {
  console.log(cmd);
  return new Promise<string>((res, rej) => {
    const prcs = exec(cmd, (err, output) => {
      err ? rej(err) : res(output);
    });
    // Show stdout live - https://stackoverflow.com/a/30084906/9788634
    prcs.stdout?.pipe(process.stdout);
  });
};

// NOTE: Use `spawn` instead of `exec` to avoid errors on long-running
// tasks
// RangeError [ERR_CHILD_PROCESS_STDIO_MAXBUFFER]: stdout maxBuffer length exceeded
const spawnCmd = (cmd: string, cmdArgs?: string[]) => {
  console.log([cmd, ...(cmdArgs ?? [])].join(" "));
  return new Promise<void>((res, rej) => {
    const prcs = spawn(cmd, cmdArgs);
    // Show stdout live - https://stackoverflow.com/a/30084906/9788634
    prcs.stdout?.pipe(process.stdout);
    prcs.on('error', (err)=> {
      rej(err);
    });
    prcs.on('exit', (code, signal) => {
      if (code != null && code > 0) rej(code);
    });
  });
};

program //
  .name(pkgJson.name)
  .description('CLI to manage actors in this monorepo')
  .version(pkgJson.version)
  .allowExcessArguments(true)
  .allowUnknownOption(true);

program
  .command('start-dev')
  .description('Run a specified actor in Development environment.')
  .argument('<actor-name>', 'Actor name (same as folder name)')
  .addHelpText('after', genExampleCall('start-dev actorName'))
  .allowExcessArguments(true)
  .allowUnknownOption(true)
  .action(async (arg, other, inst) => {
    const [actorName, ...otherArgs] = inst.args ?? [];

    const entrypoint = `${ACTOR_SRC_DIR}/${actorName}/index.ts`;
    const cmdArgs = [entrypoint, ...otherArgs];
    await spawnCmd('ts-node', cmdArgs).catch(console.error);
  });

program
  .command('start-prod')
  .description('Run a specified actor in Production environment.')
  .argument('<actor-name>', 'Actor name (same as folder name)')
  .addHelpText('after', genExampleCall('start-prod actorName'))
  .action(async (arg, other, inst) => {
    const [actorName, ...otherArgs] = inst.args ?? [];

    const entrypoint = `${ACTOR_DIST_DIR}/${actorName}/index.js`;
    const cmdArgs = [entrypoint, ...otherArgs];
    await spawnCmd('node', cmdArgs).catch(console.error);
  });

program
  .command('migrate')
  .description(
    'Run the "crawlee-one migrate" command for specified actor(s). The "dir" argument is supplied for you.'
  )
  .argument('<actor-name>', 'Actor name (same as folder name). Use "*" to select all actors.')
  .addHelpText('after', genExampleCall('migrate actorName -t v1'))
  .action(async (arg, other, inst) => {
    const [actorNameInput, ...otherArgs] = inst.args ?? [];
    const actorNames = actorNameInput === '*' ? await getAllActors() : [actorNameInput];

    for (const actor of actorNames) {
      const dir = `${ACTOR_DIST_DIR}/${actor}/migrations/migrations`;
      const cmd = `npx crawlee-one migrate --dir ${dir} ${otherArgs.join(' ')}`;
      await execCmd(cmd);
    }
  });

program
  .command('unmigrate')
  .description(
    'Run the "crawlee-one unmigrate" command for specified actor(s). The "dir" argument is supplied for you.'
  )
  .argument('<actor-name>', 'Actor name (same as folder name). Use "*" to select all actors.')
  .addHelpText('after', genExampleCall('unmigrate actorName -t v1'))
  .action(async (arg, other, inst) => {
    const [actorNameInput, ...otherArgs] = inst.args ?? [];
    const actorNames = actorNameInput === '*' ? await getAllActors() : [actorNameInput];

    for (const actor of actorNames) {
      const dir = path.normalize(`${ACTOR_DIST_DIR}/${actor}/migrations/migrations`);
      const cmd = `npx crawlee-one unmigrate --dir ${dir} ${otherArgs.join(' ')}`;
      await execCmd(cmd);
    }
  });

program
  .command('gen-actor')
  .description(
    'Run the "apify-actor-config gen" command for specified actor(s). The "config" and "out-dir" arguments are supplied for you.'
  )
  .argument('<actor-name>', 'Actor name (same as folder name). Use "*" to select all actors.')
  .addHelpText('after', genExampleCall('gen-actor actorName -s'))
  .action(async (arg, other, inst) => {
    const [actorNameInput, ...otherArgs] = inst.args ?? [];
    const actorNames = actorNameInput === '*' ? await getAllActors() : [actorNameInput];

    for (const actor of actorNames) {
      const config = path.normalize(`${ACTOR_DIST_DIR}/${actor}/config.js`);
      const outDir = path.normalize(`${ACTOR_OUT_DIR}/${actor}/.actor`);
      const cmd = `npx apify-actor-config gen --config ${config} --out-dir ${outDir} ${otherArgs.join(' ')}`; // prettier-ignore
      await execCmd(cmd);
    }
  });

program
  .command('gen-actorspec')
  .description(
    'Run the "actor-spec gen" command for specified actor(s). The "config" and "out-dir" arguments are supplied for you.'
  )
  .argument('<actor-name>', 'Actor name (same as folder name). Use "*" to select all actors.')
  .addHelpText('after', genExampleCall('gen-actorspec actorName -s'))
  .action(async (arg, other, inst) => {
    const [actorNameInput, ...otherArgs] = inst.args ?? [];
    const actorNames = actorNameInput === '*' ? await getAllActors() : [actorNameInput];

    for (const actor of actorNames) {
      const config = path.normalize(`${ACTOR_DIST_DIR}/${actor}/actorspec.js`);
      const outDir = path.normalize(`${ACTOR_OUT_DIR}/${actor}/.actor`);
      const cmd = `npx actor-spec gen --config ${config} --out-dir ${outDir} ${otherArgs.join(' ')}`; // prettier-ignore
      await execCmd(cmd);
    }
  });

program
  .command('gen-readme')
  .description(
    'Run the "readme.js" script for specified actor(s). NOTE: Output file is defined in the script.'
  )
  .argument('<actor-name>', 'Actor name (same as folder name). Use "*" to select all actors.')
  .addHelpText('after', genExampleCall('gen-readme actorName'))
  .action(async (arg, other, inst) => {
    const [actorNameInput, ...otherArgs] = inst.args ?? [];
    const actorNames = actorNameInput === '*' ? await getAllActors() : [actorNameInput];

    for (const actor of actorNames) {
      const scriptPath = path.normalize(`${ACTOR_DIST_DIR}/${actor}/readme.js`);
      // E.g. `node dist/cjs/readme.js`
      const cmd = `node ${scriptPath} ${otherArgs.join(' ')}`;
      await execCmd(cmd);
    }
  });

export const cli = () => {
  program.parse();
};
