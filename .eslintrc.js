module.exports = {
	root: true,
	env: {
		node: true,
		commonjs: true,
		es6: true,
		jquery: false,
		jest: true,
		jasmine: true,
	},
	ignorePatterns: ["dist", "coverage", "node_modules", "tmp", ".yalc", "*.d.ts"],
	parser: "@typescript-eslint/parser",
	plugins: ["@typescript-eslint"],
	extends: ["eslint:recommended", "plugin:@typescript-eslint/recommended"],
	parserOptions: {
		sourceType: "module",
	},
	rules: {
		"indent": ["error", "tab", { SwitchCase: 1 }],
		"quotes": ["error", "double"],
		"semi": ["error", "always"],
		"no-var": ["error"],
		"no-console": ["off"],
		"no-unused-vars": ["off"],
		"@typescript-eslint/no-unused-vars": ["warn"],
		"@typescript-eslint/no-explicit-any": ["warn"],
		"@typescript-eslint/ban-ts-comment": ["warn"],
		"no-mixed-spaces-and-tabs": ["error"],
	},
	overrides: [
		{
			// Plain JavaScript (jest specs, mocks, data, config): no TypeScript-only rules
			files: ["*.js"],
			parser: "espree",
			parserOptions: {
				ecmaVersion: "latest",
				sourceType: "module",
			},
			rules: {
				"@typescript-eslint/no-var-requires": ["off"],
				"@typescript-eslint/no-require-imports": ["off"],
			},
		},
	],
};
