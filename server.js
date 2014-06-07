
const ASSERT = require("assert");
const PATH = require("path");
const FS = require("fs-extra");
const URL = require("url");
const SEND = require("send");
const SMI_CACHE = require("smi.cache");
const DEEPCOPY = require("deepcopy");
const WAITFOR = require("waitfor");


var cacheBasePath = PATH.join(process.env.PIO_SERVICE_DATA_BASE_PATH, "cache");


var throttle_waiting = [];
var throttle_running = 0;
function throttle(callback, runner) {
	throttle_waiting.push([callback, runner]);
	if (throttle_waiting.length === 1) {
		(function iterate() {
			if (throttle_waiting.length === 0) return;
			if (throttle_running > 5) {
				console.log("Waiting before starting additional code path.");
				return;
			}
			throttle_running += 1;
			var task = throttle_waiting.shift();
			return task[1](function() {
				throttle_running -= 1;
				iterate();
				return callback.apply(null, Array.prototype.slice.call(arguments, 0));
			});
		})();
	}
}


require("io.pinf.server.www").for(module, __dirname, null, function(app, config) {

	ASSERT.equal(typeof config.config, "object");
	ASSERT.equal(typeof config.config.host, "string");
	ASSERT.equal(typeof config.config.catalogs, "object");

	var cache = new SMI_CACHE.UrlProxyCache(cacheBasePath, {
        ttl: 0    // Indefinite by default.
    });

	function fetchUrl(url, headers, options, callback) {

		options = options || {};

		headers = DEEPCOPY(headers || {});
		delete headers.host;
		delete headers.connection;
		delete headers.etag;

		console.log("fetch url", url);

		return cache.get(url, {
			loadBody: false,
			headers: headers,
			chown: {
				user: 1000,
				group: 1000
			},
			ttl: options.ttl || undefined,
			verbose: options.verbose || true,
			debug: options.debug || false,
			useExistingOnError: true,
			cachePath: options.cachePath || null
		}, callback);
	}

	function ensureCatalogAssets(catalogPath, catalogName, catalog, callback) {
		try {
			ASSERT.equal(typeof catalog.name, "string");
			ASSERT.equal(typeof catalog.uuid, "string");
			ASSERT.equal(typeof catalog.revision, "string");
			ASSERT.equal(typeof catalog.packages, "object");

			console.log("Downloading catalog assets for catalog: " + catalogPath + " (" + catalog.name + " / " + catalog.uuid + " / " + catalog.revision + ")");
			catalog = DEEPCOPY(catalog);

			var waitfor = WAITFOR.parallel(function(err) {
				if (err) return callback(err);
				return callback(null, catalog);
			});

			for (var packageId in catalog.packages) {
				if (catalog.packages[packageId].aspects) {
					for (var aspect in catalog.packages[packageId].aspects) {
						waitfor(packageId, aspect, function(packageId, aspect, done) {
							return throttle(done, function(done) {
								var urlParts = URL.parse(catalog.packages[packageId].aspects[aspect]);
//								var cachePath = catalogPath + "~assets~" + catalog.uuid + "/" + packageId + "~" + aspect.replace(/\[[^\]]+\]$/, "") + "~" + PATH.basename(urlParts.pathname);
								var cachePath = catalogPath.replace(/~[\w\d]{40}(~|$)/, "") + "~assets/" + PATH.basename(urlParts.pathname);
								function tryFetch(attempt) {
									return fetchUrl(catalog.packages[packageId].aspects[aspect], {}, {
										cachePath: cachePath
									}, function (err, response) {
										if (err) {
											if (attempt < 5) {
												console.error("Error while fetching '" + catalog.packages[packageId].aspects[aspect] + "' but trying again shortly:", err.stack);
												return setTimeout(function() {
													console.error("Trying fetch '" + catalog.packages[packageId].aspects[aspect] + "' again...");
													return tryFetch(attempt + 1);
												}, 3 * 1000);
											}
											return done(err);
										}
//										catalog.packages[packageId].aspects[aspect] = "http://" + config.config.host + "/catalog/" + catalogName + "~assets~" + catalog.uuid + "/" + packageId + "~" + aspect.replace(/\[[^\]]+\]$/, "") + "~" + PATH.basename(urlParts.pathname);
										catalog.packages[packageId].aspects[aspect] = "http://" + config.config.host + "/catalog/" + catalogName.replace(/~[\w\d]{40}(~|$)/, "") + "~assets/" + PATH.basename(urlParts.pathname);
										return done();
									});
								}
								return tryFetch(1);
							});
						});
					}
				}
			}
			return waitfor();
		} catch(err) {
			return callback(err);
		}
	}

	app.get(/^\/catalog\/([^\/]+?~assets)\/([^\/]+)$/, function (req, res, next) {
		return SEND(req, req.url)
			.root(cacheBasePath)
			.on('error', next)
			.pipe(res);
	});

	app.get(/^\/catalog\/([^\/]+)$/, function (req, res, next) {
		if (req.params.length === 0 || !req.params[0]) {
			res.writeHead(404);
			return res.end();
		}
		if (!req.headers["x-pio.catalog-key"]) {
			res.writeHead(400);
			return res.end("no auth code in request!");
		}
		function findCatalog(catalogName) {
			for (var name in config.config.catalogs) {
				if (/^\//.test(name)) {
					var re = new RegExp(name.replace(/(^\/|\/$)/g, ""));
					var m = re.exec(catalogName);
					if (m) {
						var catalog = DEEPCOPY(config.config.catalogs[name]);
						if (typeof m[1] !== "undefined") catalog.uri = catalog.uri.replace(/\$1/g, m[1]);
						if (typeof m[2] !== "undefined") catalog.uri = catalog.uri.replace(/\$2/g, m[2]);
						if (typeof m[3] !== "undefined") catalog.uri = catalog.uri.replace(/\$3/g, m[3]);
						return catalog;
					}
				} else
				if (name === catalogName) {
					return config.config.catalogs[name];
				}
			}
			return null;
		}
		var catalog = findCatalog(req.params[0]);
		if (!catalog) {
			console.error("catalog '" + req.params[0] + "' not configured!")
			res.writeHead(404);
			return res.end();
		}
		if (
			!catalog.headers ||
			!catalog.headers["x-pio.catalog-key"]
		) {
			console.error("'headers[x-pio.catalog-key]' not configured for catalog '" + req.params[0] + "'");
			res.writeHead(403);
			return res.end();
		}
		if (!catalog.uri) {
			console.error("'uri' not configured for catalog '" + req.params[0] + "'");
			res.writeHead(403);
			return res.end();
		}
		if (catalog.headers["x-pio.catalog-key"] !== req.headers["x-pio.catalog-key"]) {
			console.error("request auth code '" + req.headers["x-pio.catalog-key"] + "' does not match configured auth code");
			res.writeHead(403);
			return res.end("x-pio.catalog-key mismatch");
		}
		return fetchUrl(catalog.uri, req.headers, {
			cachePath: PATH.join(cacheBasePath, "catalog", req.params[0]),
			// TODO: Make this configurable.
			ttl: 15 * 1000	// Don't re-check for 15 seconds.
		}, function (err, response) {
			if (err) return next(err);
			return FS.readJson(response.cachePath, function (err, catalog) {
				if (err) return next(err);
				return ensureCatalogAssets(response.cachePath, req.params[0], catalog, function(err, catalog) {
					if (err) return next(err);
		    		var payload = JSON.stringify(catalog, null, 4);
		    		res.writeHead(200, {
		    			"Content-Type": "application/json",
		    			"Content-Length": payload.length
		    		});
		    		return res.end(payload);
				});
			});
		});
	});

	app.get(/^\/https?\//, function (req, res, next) {

		res.writeHead(403);
		return res.end("TODO: Only allow the proxying of configured URIs as well as require authentication.");

		return fetchUrl(req.url.replace(/^\/(https?)\//, "$1://"), req.headers, {}, function (err, response) {
			if (err) return next(err);
			return SEND(req, "/" + PATH.basename(response.cachePath))
				.root(PATH.dirname(response.cachePath))
				.on('error', next)
				.pipe(res);
		});
	});

});

