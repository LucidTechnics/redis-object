var rUtil = require('./lib/rutil');

var ObjectRedisAdapter = function()
{
	this.database = {};
};

ObjectRedisAdapter.prototype.convertMapToList = function(_map)
{
	var	list = [];

	for (var key in _map)
	{
		list.push(key);
		list.push(_map[key]);
	}

	return list;
};

ObjectRedisAdapter.prototype.convertMapKeysToList = function(_map)
{
	var	list = [];

	for (var key in _map)
	{
		list.push(key);
	}

	return list;
};

ObjectRedisAdapter.prototype.getMap = function(_key)
{
	var map = this.database[_key];

	if (!map)
	{
		map = {};
		this.database[_key] = map;
	}

	return map;
};

ObjectRedisAdapter.prototype.getList = function(_key)
{
	var list = this.database[_key];

	if (!list)
	{
		list = [];
		this.database[_key] = list;
	}

	return list;
};

ObjectRedisAdapter.prototype.getSet = function(_key)
{
	return this.getMap(_key);
};

ObjectRedisAdapter.prototype.getSortedSet = function(_key)
{
	var sortedSet = this.database[_key];

	if (!sortedSet)
	{
		sortedSet = {map: {}, scores: [], reverseMap: {}};
		this.database[_key] = sortedSet;
	}

	return sortedSet;
};

ObjectRedisAdapter.prototype.getValue = function(_field, _map, _defaultValue)
{
	var value = _map[_field];

	if (value === null || value === undefined)
	{
		value = _defaultValue;
		_map[_field] = value;
	}

	return value;
};

/* Returns all keys regardless of pattern ... for now */
ObjectRedisAdapter.prototype.keys = function (_pattern, _callback)
{
	//console.log("KEYS", _pattern);
	_callback && _callback(null, this.convertMapKeysToList(this.database));	
};

ObjectRedisAdapter.prototype.hincrby = function (_key, _field, _increment, _callback)
{
	//console.log("HINCRBY", _key, _field, _increment);
	
	var map = this.getMap(_key);
	
	var value = this.getValue(_field, map, 0);

	map[_field] = value + _increment;

	_callback && _callback(null, map[field]);
};

ObjectRedisAdapter.prototype.hkeys = function (_key, _callback)
{
	//console.log('HKEYS', _key);

	var keys = [];
	var map = this.getMap(_key);

	for (var key in map)
	{
		keys.push(key);
	}

	_callback && _callback(null, keys);
};

ObjectRedisAdapter.prototype.hset = function (_key, _field, _value, _callback)
{
	//console.log('HSET', _key, _field, _value);
	
	var map = this.getMap(_key);
	var returnCode = (map[_field] && 1)||0;
	map[_field] = _value;
	
	_callback && _callback(null, returnCode);
};

ObjectRedisAdapter.prototype.hmset = function (_key)
{
	//console.log('HMSET', arguments);
	
	var error;
	
	for (var i = 1, length = arguments.length; i < length; i++)
	{
		if (!error && typeof arguments[i] !== 'function')
		{
			//this is the key and i + 1 is the value
			//add key and value to map

			var field = arguments[i];
			var value = arguments[i+1];

			if (typeof value === 'function')
			{
				error = new Error('key-value mismatch detected');
			}
			
			!error && this.hset(_key, field, value);
			i++;
		}
		else
		{
			//_callback
			if (!error)
			{
				arguments[i](error, 'OK');
			}
			else
			{
				arguments[i](error);
			}
		}
	}
};

ObjectRedisAdapter.prototype.hget = function(_key, _field, _callback)
{
	//console.log('HGET', _key, _field);
	
	var map = this.getMap(_key);
	var value = this.getValue(_field, map, null);

	_callback(null, value);
};

ObjectRedisAdapter.prototype.hmget = function(_key)
{
	//console.log('HMGET', arguments);
	
	var callback;
	var results = {};
	
	for (var i = 1, length = arguments.length; i < length; i++)
	{
		if (typeof arguments[i] !== 'function')
		{
			this.hget(_key, arguments[i], function(_error, _value)
			{
				results[arguments[i]] = _value;
			});
		}
		else
		{
			//_callback
			arguments[i](null, results);
		}
	}	
};

ObjectRedisAdapter.prototype.hgetall = function(_key, _callback)
{
	//console.log('HGETALL', _key);
	
	_callback && _callback(null, this.getMap(_key));
};

ObjectRedisAdapter.prototype.sadd = function (_key, _callback)
{
	//console.log('SADD', arguments);
	
	var set = this.getSet(_key);
	var memberCount = 0;
	
	for (var i = 1, length = arguments.length; i < length; i++)
	{
		if (typeof arguments[i] !== 'function')
		{
			set[arguments[i]] && memberCount++;
			set[arguments[i]] = 1;
		}
		else
		{
			//_callback
			arguments[i](null, memberCount);
		}
	}
};

ObjectRedisAdapter.prototype.incr = function (_key, _callback)
{
	//console.log('INCR', _key);
	
	this.incrby(_key, 1, _callback);
};

ObjectRedisAdapter.prototype.set = function (_key, _value, _callback)
{
	//console.log('SET', _key, _value);
	
	this.database[_key] = _value;
	_callback && _callback(null, 'OK');
};

ObjectRedisAdapter.prototype.incrby = function (_key, _increment, _callback)
{
	//console.log('INCRBY', _key, _increment);
	
	var error, value = this.getValue(_key, this.database, 0), valueType = typeof value;
	
	if (valueType !== 'string' && valueType !== 'number')
	{
		error = new Error('Invalid value for key ' + _key);
	}
	else if (valueType === 'string')
	{
		var value = parseInt(value, 10);

		if (value === NaN)
		{
			error = new Error('Invalid string value for key ' + _key);
		}
	}

	if (!error)
	{
		this.set(_key, value + _increment);
		
		_callback && _callback(null, this.database[_key]);
	}
	else
	{
		_callback && _callback(error);
	}

};

ObjectRedisAdapter.prototype.smembers = function (_key, _callback)
{
	//console.log('SMEMBERS', _key);
	
	_callback && _callback(null, this.convertMapKeysToList(this.getSet(_key)));	
};

ObjectRedisAdapter.prototype.get = function(_key, _callback)
{
	//console.log('GET', _key);
	
	var value = this.getValue(_key, this.database, null);

	_callback && _callback(null, _value);
};

ObjectRedisAdapter.prototype.exists = function(_key, _callback)
{
	//console.log('EXISTS', _key);
	
	var callback = function(_error, _keys)
	{
		//This is interesting ... if the key exists and has value
		//'false' a value check for existence will return false.
		//In order to fix this, run through the entire set of keys in
		//the map to see if the key is present.
		var exists = false;

		if (!_error)
		{
			for (var i = 0, length = keys.length; i < length; i++)
			{
				if (_keys[i] === _key)
				{
					exists = true;
					break;
				}
			}
		}

		_callback(_error, exists);
	}
	
	_callback && this.keys('*', callback);
};

ObjectRedisAdapter.prototype.zadd = function (_key)
{
	//console.log('ZADD', arguments);

	var error, oldMemberPosition = null, memberCount = 0, callback, addedMemberCount = 0;
	
	if (typeof _key !== 'string')
	{
		error = new Error('key must be a string');
	}

	if (!error)
	{
		//console.log('no error');
		var sortedSet = this.getSortedSet(_key);

		for (var i = 1, length = arguments.length; i < length; i++)
		{
			var score = arguments[i];
			var member = arguments[i+1];

			if (typeof score === 'function')
			{
				callback = score;
			}
			else if (typeof member !== 'string' && typeof member !== 'number')
			{
				error = new Error('member must be a string');
			}
			else
			{
				member = member + '';
				
				if (typeof score !== 'number' && typeof score !== 'string')
				{
					error = new Error('score must be a number or a string representation of a number');
				}
				else if (typeof score === 'string')
				{
					score = parseFloat(score);

					if (score === NaN)
					{
						error = new Error('score is a string but not a string representation of a number');
					}
				}

				if (rUtil.isDefined(sortedSet.map[member]) === true)
				{
					oldMemberPosition = sortedSet.map[member];
					var memberMap = sortedSet.reverseMap[sortedSet.scores[oldMemberPosition]];
					delete memberMap[member];

					if (rUtil.isEmpty(memberMap) === true)
					{
						delete sortedSet.reverseMap[sortedSet.scores[oldMemberPosition]];
					}

					sortedSet.scores.splice(oldMemberPosition, 1);
				}
				else
				{
					addedMemberCount++;
				}

				var insertionPoint = null;
				
				for (var j = 0, jLength = sortedSet.scores.length; j < jLength; j++)
				{
					var candidateScore = sortedSet.scores[j];
					
					if (candidateScore >= score)
					{
						insertionPoint = j;
						sortedSet.scores.splice(j, 0, score);
						sortedSet.map[member] = j;
						break;
					}
				}

				if (insertionPoint === null)
				{
					sortedSet.scores.push(score);
					sortedSet.map[member] = sortedSet.scores.length - 1;
					insertionPoint = sortedSet.scores.length - 1;
				}

				sortedSet.reverseMap[score] = sortedSet.reverseMap[score]||{};
				sortedSet.reverseMap[score][member] = 1;

				if ((oldMemberPosition === null && insertionPoint !== (sortedSet.scores.length - 1)) || (oldMemberPosition !== null && oldMemberPosition !== insertionPoint))
				{
					for (var adjustedMember in sortedSet.map)
					{
						if (adjustedMember !== member)
						{
							if (oldMemberPosition !== null)
							{
								if (oldMemberPosition > insertionPoint)
								{
									if (sortedSet.map[adjustedMember] >= insertionPoint && sortedSet.map[adjustedMember] < oldMemberPosition)
									{
										sortedSet.map[adjustedMember] = sortedSet.map[adjustedMember] + 1;
									}
								}
								else if (oldMemberPosition < insertionPoint)
								{
									if (sortedSet.map[adjustedMember] < insertionPoint && sortedSet.map[adjustedMember] >= oldMemberPosition)
									{
										sortedSet.map[adjustedMember] = sortedSet.map[adjustedMember] - 1;
									}
								}
							}
							else if (sortedSet.map[adjustedMember] >= insertionPoint)
							{
								sortedSet.map[adjustedMember] = sortedSet.map[adjustedMember] + 1;
							}
						}
					}
				}
			}

			i++;
		}
	}

	callback && callback(error, addedMemberCount);
};

ObjectRedisAdapter.prototype.zincrby = function (_key, _increment, _member, _callback)
{
	//console.log('ZINCRBY', _key, _increment, _member);
	
	var sortedSet = this.getSortedSet(_key);

	var score = null;

	if (rUtil.isDefined(sortedSet.map[member]) === true)
	{
		var score = sortedSet.scores[sortedSet.map[_member]];
		this.zadd(_key, score + _increment, _member, _callback);
	}
};

ObjectRedisAdapter.prototype.processZFunctionParameters = function(_parameters)
{
	//console.log('PROCESSZFUNCTIONPARAMETERS', _parameters);
	
	var parameters = {offset: 0, count: 1000000000, withScores: false};

	var determineRangeValue = function(_parameterValue)
	{
		var rangeValue = _parameterValue;

		if ('-inf' === _parameterValue)
		{
			rangeValue = -1000000000
		}
		else if ('+inf' === _parameters[1])
		{
			rangeValue = 1000000000
		}

		return rangeValue;
	};

	var parameter0 = determineRangeValue(_parameters[0]);
	var parameter1 = determineRangeValue(_parameters[1]);

	if (parameter0 < parameter1)
	{
		parameters.min = parameter0;
		parameters.max = parameter1;
	}
	else
	{
		parameters.min = parameter1;
		parameters.max = parameter0;
	}

	if (_parameters[2] && (typeof _parameters[2] === 'string') && _parameters[2].toUpperCase() === 'WITHSCORES')
	{
		parameters.withScores = true;

		if (_parameters[3] && (typeof _parameters[3] === 'string') &&  _parameters[3].toUpperCase() === 'LIMIT')
		{
			parameters.offset = _parameters[4];
			parameters.count = _parameters[5];
			parameters.callback = _parameters[6];
		}
		else if (typeof _parameters[3] === 'function')
		{
			parameters.callback = _parameters[3];
		}
	}
	else if (_parameters[2] && (typeof _parameters[2] === 'string') && _parameters[2].toUpperCase() === "LIMIT")
	{
		parameters.offset = _parameters[3];
		parameters.count = _parameters[4];
		parameters.callback = _parameters[5];
	}
	else if (typeof _parameters[2] === 'function')
	{
		parameters.callback = _parameters[2];
	}

	return parameters;
};

ObjectRedisAdapter.prototype.zrangebyscore = function(_key)
{
	//console.log('ZRANGEBYSCORE', arguments);
	
	var sortedSet = this.getSortedSet(_key);
	var resultCount = 0;
	var results = [];

	var parameters = this.processZFunctionParameters(Array.prototype.slice.call(arguments, 1));

	for (var i = 0, length = sortedSet.scores.length; i < length; i++)
	{
		var score = sortedSet.scores[i];

		if (score >= parameters.min && score <= parameters.max && resultCount < parameters.count && i >= parameters.offset)
		{
			var memberMap = sortedSet.reverseMap[score];
			var memberCount = rUtil.size(memberMap);

			for (var member in memberMap)
			{
				resultCount++;

				if (resultCount < parameters.count)
				{
					results.push(member);
					parameters.withScores && results.push(score);
				}
				else
				{
					break;
				}
			}

			i += memberCount - 1;
		}

		if (resultCount >= parameters.count)
		{
			break;
		}
	}

	parameters.callback(null, results);
};

ObjectRedisAdapter.prototype.zrevrangebyscore = function(_key)
{
	//console.log('ZREVRANGEBYSCORE', arguments);
	
	var sortedSet = this.getSortedSet(_key);
	var resultCount = 0;
	var results = [];

	var parameters = this.processZFunctionParameters(Array.prototype.slice.call(arguments, 1));

	for (var i = sortedSet.scores.length - 1, length = sortedSet.scores.length; i >= 0; i--)
	{
		var score = sortedSet.scores[i];

		if (score >= parameters.min && score <= parameters.max && resultCount < parameters.count && i < (length - parameters.offset))
		{
			var memberMap = sortedSet.reverseMap[score];
			var memberCount = rUtil.size(memberMap);

			for (var member in memberMap)
			{
				resultCount++;

				if (resultCount < parameters.count)
				{
					results.push(member);
					parameters.withScores && results.push(score);
				}
				else
				{
					break;
				}
			}

			i -= memberCount - 1;
		}

		if (resultCount >= parameters.count)
		{
			break;
		}
	}

	parameters.callback(null, results);
};

ObjectRedisAdapter.prototype.on = function()
{
	throw new Error('Not implemented');
};

ObjectRedisAdapter.prototype.select = function(_key) {};

ObjectRedisAdapter.prototype.quit = function() {};

exports.create = function(_serverMap, _log)
{
	return new ObjectRedisAdapter();
};

exports.print = function (_error, _result)
{
	_error && console.log("Error: " + _error);
    _result && console.log("Reply: " + _result);
};