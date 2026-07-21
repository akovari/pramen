// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://akovari.github.io',
	base: '/pramen',
	integrations: [
		starlight({
			title: 'Pramen',
			description:
				'A lean, columnar data movement runtime with governed LLM enrichment. One static binary, one YAML file, enriched rows in PostgreSQL.',
			logo: {
				src: './src/assets/logo.svg',
				replacesTitle: false,
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/akovari/pramen' },
			],
			customCss: ['./src/styles/custom.css'],
			editLink: {
				baseUrl: 'https://github.com/akovari/pramen/edit/main/site/',
			},
			sidebar: [
				{
					label: 'Getting started',
					items: [
						{ label: 'Introduction', slug: 'getting-started/introduction' },
						{ label: 'Installation', slug: 'getting-started/installation' },
						{ label: 'Quickstart', slug: 'getting-started/quickstart' },
					],
				},
				{
					label: 'Concepts',
					items: [
						{ label: 'Architecture', slug: 'concepts/architecture' },
						{ label: 'The pipeline document', slug: 'concepts/pipeline-spec' },
						{ label: 'Runtime and delivery', slug: 'concepts/runtime' },
						{ label: 'Governed AI enrichment', slug: 'concepts/governed-ai' },
					],
				},
				{
					label: 'Cookbook',
					items: [
						{ label: 'Filter and derive with SQL', slug: 'cookbook/filter-and-derive' },
						{ label: 'Loading PostgreSQL fast', slug: 'cookbook/postgres-loading' },
						{ label: 'Testing pipelines locally', slug: 'cookbook/local-testing' },
						{ label: 'Budgeted AI extraction', slug: 'cookbook/ai-extraction' },
						{ label: 'Incremental re-enrichment', slug: 'cookbook/incremental-enrichment' },
						{ label: 'S3 and MinIO sources', slug: 'cookbook/s3-sources' },
						{ label: 'Object-store sources (S3, Azure, GCS)', slug: 'cookbook/object-store-sources' },
						{ label: 'WASM transforms', slug: 'cookbook/wasm-transforms' },
						{ label: 'Deploying on AWS', slug: 'cookbook/aws-deploy' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Pipeline schema', slug: 'reference/pipeline-schema' },
						{ label: 'CLI', slug: 'reference/cli' },
					],
				},
				{
					label: 'Project',
					items: [
						{ label: 'Status and roadmap', slug: 'project/roadmap' },
						{ label: 'Compared to alternatives', slug: 'project/comparison' },
						{ label: 'Measured results', slug: 'project/benchmarks' },
						{ label: 'Dispatch policy', slug: 'project/dispatch-policy' },
						{ label: 'Design decisions', slug: 'project/decisions' },
					],
				},
			],
		}),
	],
});
