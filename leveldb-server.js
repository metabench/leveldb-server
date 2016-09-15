/**
 * Created by James on 15/09/2016.
 */

// Could use jsgui server capabilities to handle URL encoding and some other things.

// Level server could act as a resource, or extend jsgui server.
//  jsgui server is a resource?
//  not right now.


var http = require('http');
var url = require('url');

// single key / value get or put record, delete
// query to get list of records from what their keys begin with
//  ?key_begins
// queries to get the keys before or after any given key
//  ?key_before, ?key_after

// /kvs/[key]
// /kvs/?query


var levelup = require('level');

// /kvs/  JSON encoded POST data, with no specific URL

// Could make this an object that is controllable by something else.

// Get all keys?

// Get all keys beginning...

// Nice to have some replication / rebuilding features...
//  stream_all
//   should stream every record as JSON (through HTTP)
//  get_all
//   stream all records as JSON through HTTP.
//  subscribe(_to_updates)

class LevelDB_Server {
	constructor(db_path, port) {
		this.db_path = db_path;
		this.port = port;
	}
	start(callback) {
		var db = this.db = levelup(this.db_path);

		var get_all_keys = function(callback) {
			var res_keys = [];

			db.createKeyStream().on('data', function(key) {
					//console.log(data.key, '=', data.value)
					res_keys.push(decodeURI(key));
				})
				.on('error', function (err) {
					//console.log('Oh my!', err)
					callback(err);
				})
				.on('close', function () {
					//console.log('Stream closed')
				})
				.on('end', function () {
					callback(null, res_keys);
				})
		}


		var get_data_keys_ranging = function(lower, upper, callback) {
			var res_records = [];

			db.createReadStream({
				'gte': encodeURI(lower),
				'lte':  encodeURI(upper),
			}).on('data', function (data) {
					//console.log(data.key, '=', data.value)

					//console.log('')
					res_records.push([decodeURI(data.key), data.value]);

				})
				.on('error', function (err) {
					//console.log('Oh my!', err)
					callback(err);
				})
				.on('close', function () {
					//console.log('Stream closed')
				})
				.on('end', function () {
					//console.log('Stream closed')

					//console.log('res_records', res_records);

					callback(null, res_records);

				})
		}

		var get_keys_ranging = function(lower, upper, callback) {
			var res_records = [];

			db.createKeyStream({
				'gte': encodeURI(lower),
				'lte':  encodeURI(upper),
			}).on('data', function (key) {
					//console.log(data.key, '=', data.value)
					//console.log('')
					res_records.push(key);
				})
				.on('error', function (err) {
					//console.log('Oh my!', err)
					callback(err);
				})
				.on('close', function () {
					//console.log('Stream closed')
				})
				.on('end', function () {
					//console.log('Stream closed')

					//console.log('res_records', res_records);

					callback(null, res_records);

				})
		};

		var get_keys_beginning = function(key_prefix, callback) {
			get_keys_ranging(key_prefix + ' ', key_prefix + '~', callback);
		};

		// Could push 1000 records at a time in the streaming JSON.



		var get_data_multi_callbacks = function(num_per_callback, cb_data, cb_end) {
			var res_records = [];
			var c_records = 0;
			var stream = db.createReadStream()

			var pause = function() {
				stream.pause();
			}
			var resume = function() {
				stream.resume();
			}


			stream.on('data', function (data) {
					//console.log(data.key, '=', data.value)

					//console.log('')
					res_records.push([decodeURI(data.key), data.value]);
					c_records++;
					if (c_records === num_per_callback) {
						cb_data(res_records, pause, resume);
						c_records = 0;
						res_records = [];
					}



				})
				.on('error', function (err) {
					//console.log('Oh my!', err)
					callback(err);
				})
				.on('close', function () {
					//console.log('Stream closed')
				})
				.on('end', function () {

					if (c_records > 0) {
						cb_data(res_records, pause, resume);
					}


					//console.log('Stream closed')

					//console.log('res_records', res_records);

					//callback(null, res_records);
					cb_end();

				})
		};


		var get_data_keys_beginning = function(key_prefix, callback) {
			get_data_keys_ranging(key_prefix + ' ', key_prefix + '~', callback);
		}

		var server = http.createServer(function(request, response) {




			var srvUrl = url.parse(`http://${request.url}`);
			//console.log('srvUrl', srvUrl);

			// Make it so the whole path is just part of the key (unless we use a query).


			// /kvs/restful interface to keys and values
			//  Keys and values can have URL encoding that gets ignored.

			// query/key_beginning/[key]all further url encoding gets ignored
			// query/next/[key]

			var path = srvUrl.path;
			//console.log('path', path);


			var pos1 = path.indexOf('/', 1);

			if (pos1 > 0) {
				var path1 = path.substr(1, pos1 - 1);

				//console.log('path1', path1);

				if (path1 === 'kvs') {
					var key = path.substr(pos1 + 1);

					// read the value from the request body.
					//  binary value?
					//  header that says how the value is encoded?

					var body = request.body;

					//console.log('key', key);
					//console.log('body', body);

					var method = request.method;

					//console.log('method', method);

					var content_length = parseInt(request.headers['content-length'], 10);
					//console.log('content_length', content_length);

					if (request.body) {

					} else {
						if (method === 'GET') {
							db.get(key, function (err, value) {
								if (err) {
									//console.log('typeof err', typeof err);

									if ((err + '').substr(0, 13) === 'NotFoundError') {
										//console.log ('key not found');

										response.writeHead(404, {"Content-Type": "text/plain"});
										response.write("404 Not Found\n");
										response.end();

									} else {
										return console.log('DB get error', err);
										throw err;

									}

								} else {
									//console.log('name=' + value);

									// Write it as binary

									//console.log('typeof value', typeof value);

									response.writeHead(200, {"content-type": "application/octet", 'content-length': value.length});
									response.write(value);
									response.end();

								}// likely the key was not found

								// ta da!

							});

						} else if (method === 'PUT') {
							//if (request.method == 'POST') {
							// A buffer?
							//console.log('content_length', content_length);
							//var body = '';
							var buf_body = Buffer.alloc(content_length);
							//var buf_body = new Buffer(content_length);
							var write_pos = 0;

							request.on('data', function(data) {
								//body += data;
								//console.log('data.length', data.length);
								data.copy(buf_body, write_pos);
								write_pos = write_pos + data.length;

								//console.log('data', data);
								//console.log('data ' + data);
								//buf_body.write(data + '');
								// Too much POST data, kill the connection!
								// 1e6 === 1 * Math.pow(10, 6) === 1 * 1000000 ~~~ 1MB
								//if (body.length > 1e6)
								//	request.connection.destroy();
							});

							request.on('end', function() {
								//var post = qs.parse(body);

								//console.log('buf_body', buf_body);
								//console.log('buf_body', buf_body + '');
								//console.log('buf_body.length', buf_body.length);

								// Something to confirm it was set correctly?

								// return a binary 1

								// application/octet-stream


								db.put(key, buf_body, function (err, value) {
									if (err) {

										//console.log('typeof err', typeof err);

										if ((err + '').substr(0, 13) === 'NotFoundError') {
											//console.log ('key not found');

											response.writeHead(404, {"Content-Type": "text/plain"});
											response.write("404 Not Found\n");
											response.end();

										} else {
											return console.log('Ooops!', err);
										}


									} else {
										//console.log('name=' + value);

										var buf_res = Buffer.alloc(1);
										buf_res[0] = 1;



										response.writeHead(200, {"content-type": "application/octet", 'content-length': '1'});
										response.write(buf_res);
										response.end();



									}// likely the key was not found

									// ta da!

								});


								// use post['blah'], etc.
							});
							//
						}
					}
				}

				if (path1 === 'query') {
					// query/[queryname]
					//  payload is in the request body

					// May query for records with keys beginning with something.

					// Just query for the keys?

					// Keys beginning could look for between key+low ascii and key + high ascii

					// Could add a character 0

					// Need to find out what the query name is.

					//query/?keysbeginning=key
					// query/keys?keys_beginning=...

					var query = path.substr(pos1 + 1);
					//console.log('query', query);

					var pos2 = path.indexOf('?', pos1 + 1);
					var query_word = path.substring(pos1 + 1, pos2);

					//console.log('query_word', query_word);

					// Then split it up by &

					var str_query_params = path.substr(pos2 + 1);

					//console.log('str_query_params', str_query_params);

					var arr_query_params = str_query_params.split('&');

					//console.log('arr_query_params', arr_query_params);

					var arr_query_keys_and_values = [];
					arr_query_params.forEach(function(v) {
						arr_query_keys_and_values.push(decodeURI(v).split('='));
					});

					//console.log('arr_query_keys_and_values', arr_query_keys_and_values);

					// requesting an object indexed set of records for a batch of items.

					// also want to express a collection of the keys to retrieve
					//  JSON encoded array in URL

					// query/record_ranges?key_ranges=[[a,b],[c,d],...]

					// All data
					//  keys and values.
					//  Lets stream it, as there may be quite a lot.
					//   Find out about http pipe where sending gets slowed if it does not get recieved fast enough...
					//    or delay iteration effectively, or delay sending.
					//  Could have a download speed header which it looks at. When copying DB could go for a consistant but fast speed eg 50MB/s
					var is_first = true;

					if (query === 'all') {
						// Iterate all of them
						//  A custom iterator?

						// Something to slow down the iteration...


						get_data_multi_callbacks(256, function(arr_data, pause, resume) {
							//console.log('arr_data.length', arr_data.length);
							var json_data = JSON.stringify(arr_data);

							//console.log('json_data', json_data);
							//console.log('json_data 0 ', json_data.charCodeAt(0));
							//console.log('json_data 1 ', json_data.charCodeAt(1));
							//console.log('json_data 2 ', json_data.charCodeAt(2));
							//console.log('json_data 3 ', json_data.charCodeAt(3));
							//console.log('json_data 4 ', json_data.charCodeAt(4));

							// remove the first and last characters.
							//console.log('json_data.length', json_data.length);

							var internal_data = json_data.slice(1, -1);

							if (is_first) {
								response.writeHead(200, {"content-type": "application/json"});
								response.write('[');
								pause();
								response.write(internal_data, function() {


									setTimeout(function() {
										resume();
									}, 20);
									//resume();
								});

								is_first = false;

								// And cork the readable stream until the output data has been flushed??



							} else {
								pause();
								response.write(internal_data, function() {

									// Slows down the writing, gives chance to catch up.



									setTimeout(function() {
										resume();
									}, 20);

								});

							}




							//throw 'stop'
						}, function() {
							response.write(']');
							response.end();
							// end
						});

					}



					if (query === 'all_keys') {
						get_all_keys(function(err, res_all_keys) {
							if (err) {
								callback(err);
							} else {
								//console.log('res_all_keys.length', res_all_keys);

								// Could take too long to stringify all keys.

								var json_all_keys = JSON.stringify(res_all_keys);

								response.writeHead(200, {"content-type": "application/json", 'content-length': json_all_keys.length});
								response.write(json_all_keys);
								response.end();
							}
						})
					}

					if (query_word === 'values') {
						// Then we need to look for the ranges.

						//throw 'stop';
						if (arr_query_keys_and_values.length === 1) {
							if (arr_query_keys_and_values[0][0] === 'key_ranges') {



								var arr_ranges = JSON.parse(arr_query_keys_and_values[0][1]);
								var num_retreivals = arr_ranges.length;
								var errs = [];
								var res = [];

								arr_ranges.forEach(function (arr_range) {
									//
									get_data_keys_ranging(arr_range[0], arr_range[1], function (err, res_range) {
										if (err) {
											errs.push(err);
										} else {
											//console.log('arr_range', arr_range);
											//throw 'stop';

											//console.log('res_range', res_range);

											//res.push([arr_range, res_range]);
											res = res.concat(res_range);

											num_retreivals--;

											if (num_retreivals === 0) {
												var json_res = JSON.stringify(res);
												//console.log('res', res);

												//console.log('json_res', json_res.substr(0, 200));

												response.writeHead(200, {
													"content-type": "application/json",
													'content-length': json_res.length
												});
												response.write(json_res);
												response.end();

												//console.log('written response');


											}
										}
									})


								});
							}
						}
					}


					if (query_word === 'record_ranges') {
						if (arr_query_keys_and_values.length === 1) {
							if (arr_query_keys_and_values[0][0] === 'ranges') {
								//console.log('arr_query_keys_and_values[0][1]', arr_query_keys_and_values[0][1]);
								//throw 'stop';
								var arr_ranges = JSON.parse(arr_query_keys_and_values[0][1]);
								var num_retreivals = arr_ranges.length;
								var errs = [];
								var res = [];

								arr_ranges.forEach(function(arr_range) {
									//
									get_data_keys_ranging(arr_range[0], arr_range[1], function(err, res_range) {
										if (err) {
											errs.push(err);
										} else {
											//console.log('arr_range', arr_range);
											//throw 'stop';

											//console.log('res_range', res_range);

											res.push([arr_range, res_range]);
											num_retreivals--;

											if (num_retreivals === 0) {
												var json_res = JSON.stringify(res);
												//console.log('res', res);

												//console.log('json_res', json_res.substr(0, 200));

												response.writeHead(200, {"content-type": "application/json", 'content-length': json_res.length});
												response.write(json_res);
												response.end();


											}
										}
									})
								});
							}

						}
						//console.log('arr_query_keys_and_values', arr_query_keys_and_values);
						//throw 'stop';
					}

					if (query_word === 'records') {
						if (arr_query_keys_and_values.length === 1) {
							if (arr_query_keys_and_values[0][0] === 'keys_beginning') {
								var key_prefix = arr_query_keys_and_values[0][1];

								get_data_keys_beginning(key_prefix, function(err, res_records) {
									var json_records = JSON.stringify(res_records);

									response.writeHead(200, {"content-type": "application/json", 'content-length': json_records.length});
									response.write(json_records);
									response.end();
								})

							}
						}
					}
					if (query_word === 'keys') {
						if (arr_query_keys_and_values.length === 1) {
							if (arr_query_keys_and_values[0][0] === 'keys_beginning') {



								var key_prefix = arr_query_keys_and_values[0][1];

								get_keys_beginning(key_prefix, function(err, res_records) {
									var json_records = JSON.stringify(res_records);

									response.writeHead(200, {"content-type": "application/json", 'content-length': json_records.length});
									response.write(json_records);
									response.end();
								})
							}
						}
					}

					if (query_word === 'count') {
						if (arr_query_keys_and_values.length === 1) {
							if (arr_query_keys_and_values[0][0] === 'keys_beginning') {
								var key_prefix = arr_query_keys_and_values[0][1];

								//var res_records = [];
								var res_count = 0;

								db.createReadStream({
									'gte': key_prefix + ' ',
									'lte':  key_prefix + '~',
								}).on('data', function (data) {
										//console.log(data.key, '=', data.value)
										//res_records.push([decodeURI(data.key), data.value.split('|')]);
										res_count++;

									})
									.on('error', function (err) {
										console.log('Oh my!', err)
									})
									.on('close', function () {
										console.log('Stream closed')
									})
									.on('end', function () {
										console.log('Stream closed')

										var json_count = JSON.stringify(res_count);

										response.writeHead(200, {"content-type": "application/json", 'content-length': json_count.length});
										response.write(json_count);
										response.end();

									})
							}
						}
					}
				}
			} else {
				response.writeHead(404, {"Content-Type": "text/plain"});
				response.write("404 Not Found\n");
				response.end();
			}
			//response.end();
		});

		//console.log('pre callback');
		callback(null, server);

		server.listen(this.port);
		console.log("Server is listening on port " + this.port + ', using database path ' + this.db_path);

	}

}


if (require.main === module) {

	// Require a database path / choose a default path
	//  Easiest setup possible is best, allow config options too

	//console.log('process.argv', process.argv);
	var db_path;
	var port = 420;

	// Is the first one the node executable?



	if (process.argv.length === 2) {
		db_path = process.argv[1];
	}
	if (process.argv.length === 3) {
		db_path = process.argv[2];
	}
	if (process.argv.length === 4) {
		db_path = process.argv[2];
		port = parseInt(process.argv[3]);
	}

	//console.log('db_path', db_path);


	// Could have default port.
	//  Question about where to store data file when the server is installed.
	//  Should be possible to keep it in the local node module's directory.
	//   However, that could make it very big...
	//    But its best not to copy large amounts of modules anyway.
	//     Storing it somewhere within the node modules / global node modules makes sense.

	var ls = new LevelDB_Server(db_path, port);

	ls.start((err, res_started) => {
		if (err) {
			throw err;
		} else {
			//console.log('Started Level_Server');
		}
	})

} else {
	//console.log('required as a module');
}


module.exports = LevelDB_Server;