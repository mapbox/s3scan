#! /usr/bin/env node

import { Command } from 'commander'
import * as figlet from 'figlet'
import * as packageJson from '../../package.json';
import { S3Scan } from '../lib'
import * as http from 'http'


const program = new Command()
const version = packageJson.version

console.log(figlet.textSync(`S3 Scan ${version}`))


program
    .version(version)
    .description("A tool to list all keys in a bucket/prefix")
    .usage(`Usage: s3scan <s3url> [--concurrency=num] [--gunzip]`)
    .argument('<s3Url>', 'an S3 uri of the type s3://bucket/prefix')
    .option('-g, --gunzip [boolean]', 'boolean argument to unzip the data from stream', false)
    .option("-c, --concurrency  [number]", "Concurrency to use")
    .parse(process.argv);

var agent = new http.Agent({
    keepAlive: true,
    maxSockets: Math.ceil(require('os').cpus().length * 16),
    keepAliveMsecs: 60000
})


const options = { agent: agent, body: true, ...program.opts() }
const s3Url = program.args[0]
S3Scan.Scan(s3Url, options).pipe(process.stdout);
