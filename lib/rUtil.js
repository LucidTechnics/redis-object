var crypto = require('crypto');
var _ = require('underscore');
_.mixin(require('underscore.string'));

var escapeCode = {};

//before changing or adding to these please look at
//http://www.ascii.cl/htmlcodes.htm first!!!

//numeric codes
escapeCode['038;'] = function() { return ' '; }; //normalize '&' to whitespace
escapeCode['039;'] = function() { return "'"; };
escapeCode['043;'] = function() { return ' '; }; //normalize '+' to whitespace
escapeCode['160;'] = function() { return ' '; };
escapeCode['167;'] = function() { return ' '; };
escapeCode['169;'] = function() { return ' phoenix-copyright-symbol '; };
escapeCode['174;'] = function() { return ' phoenix-registered-trademark-symbol '; }
escapeCode['183;'] = function() { return ','; }; // this is a middot (German comma) and not a period.
escapeCode['190;'] = function() { return ' '; };
escapeCode['223;'] = function() { return ' '; };
escapeCode['233;'] = function() { return ' '; };
escapeCode['8211;'] = function() { return '-'; }; // normalize the en dash to a dash
escapeCode['8212;'] = function() { return '-'; }; //normalize the em dash to a dash
escapeCode['8216;'] = function() { return "'"; }; //map this to the standard single quotation mark
escapeCode['8217;'] = function() { return "'"; }; //map this to the standard single quotation mark
escapeCode['8220;'] = function() { return '"'; }; //double left quotation mark
escapeCode['8221;'] = function() { return '"'; }; //double right quotation mark
escapeCode['8226;'] = function() { return '-'; }; //normalize the bullet to a dash
escapeCode['8482;'] = function() { return '™'; }; //strange character is the trademark symbol
escapeCode['#151;'] = function() { return '-'; }; //substituting a hyphen value for this special character long hyphen
escapeCode['#146;'] = function() { return "'"; }; //map this to the standard single quotation mark

//letter codes
escapeCode['amp;'] = function() { return this['038;'](); };
escapeCode['apos;'] = function() { return this['8216;'](); };
escapeCode['quot;'] = function() { return this['8216;'](); };
escapeCode['nbsp;'] = function() { return this['160;'](); };
escapeCode['copy;'] = function() { return this['169;'](); };
escapeCode['reg;'] = function() { return this['174;'](); };
escapeCode['#xd;'] = function() { return ' '; };

//hex codes
escapeCode['xA0;'] = function() { return this['reg;'](); };
escapeCode['x2013;'] = function() { return this['8211;'](); };
escapeCode['x2014;'] = function() { return this['8212;'](); };
escapeCode['x2018;'] = function() { return this['8216;'](); };
escapeCode['x2019;'] = function() { return this['8217;'](); };
escapeCode['x201C;'] = function() { return this['8220;'](); };
escapeCode['x201D;'] = function() { return this['8221;'](); };

//unknown codes
escapeCode['65293;'] = function() { return ' '; };
escapeCode['8213;'] = function() { return ' '; };
escapeCode['8364;'] = function() { return ' '; };
escapeCode['8540;'] = function() { return ' '; };
escapeCode['8542;'] = function() { return ' '; };
escapeCode['8722;'] = function() { return ' '; };
escapeCode['9679;'] = function() { return ' '; };
escapeCode['xE9;'] = function() { return ' '; };
escapeCode['#13;'] = function() { return ' '; };
escapeCode['95;'] = function() { return ' '; };
escapeCode['91;'] = function() { return ' '; };
escapeCode['93;'] = function() { return ' '; };
escapeCode['9679;'] = function() { return ' '; };
escapeCode['xE9;'] = function() { return ' '; };
escapeCode['#13;'] = function() { return ' '; };

exports.escapeChar = function(_escapeCode)
{
	var escapeCharacter = ' ';
	var functionName = _escapeCode.substring(1);

	if (escapeCode[functionName])
	{
		escapeCharacter = escapeCode[functionName]();
	}

	return escapeCharacter;
};

exports.hash = function(_string, _salt)
{
	var md5Hash = crypto.createHash('md5');

	md5Hash.update(_string);
	_salt && md5Hash.update(_salt);

	return md5Hash.digest('hex');
};

exports.times = function(_number, _callback)
{
	for (var i = 0; i < _number; i++)
	{
		_callback(i);
	}
};

exports.merge = function(_object1, _object2)
{
	if (_object1 && _object2)
	{
		for (var attrname in _object1)
		{
			_object2[attrname] = _object1[attrname];
		}
	}

	return _object2||_object1||{};
};

exports.partialMerge = function(_object1, _object2)
{
	if (_object1 && _object2)
	{
		for (var attrname in _object1)
		{
			if(!_object2[attrname])
			{
				_object2[attrname] = _object1[attrname];
			}
		}
	}

	return _object2||_object1||{};
};

exports.size = function(_object)
{
	var size = 0;

	for (var i in _object)
	{
		size++;
	}

	return size;
};

exports.isEmpty = function(_object)
{
	var isEmpty = true;
	
	for (var i in _object)
	{
		isEmpty = false;
		break;
	}

	return isEmpty;
};

exports.isDefined = function(_thing)
{
	var isDefined = false;

	if (_thing !== undefined && _thing !== null)
	{
		isDefined = true;
	}

	return isDefined;
};

// LZW-compress a string
exports.lzw_encode = function(s)
{
	var dict = {};
	var data = (s + "").split("");
	var out = [];
	var currChar;
	var phrase = data[0];
	var code = 256;
	
	for (var i=1; i<data.length; i++) {
		currChar=data[i];
		if (dict[phrase + currChar] != null) {
			phrase += currChar;
		}
		else {
			out.push(phrase.length > 1 ? dict[phrase] : phrase.charCodeAt(0));
			dict[phrase + currChar] = code;
			code++;
			phrase=currChar;
		}
	}
	out.push(phrase.length > 1 ? dict[phrase] : phrase.charCodeAt(0));
	for (var i=0; i<out.length; i++) {
		out[i] = String.fromCharCode(out[i]);
	}
	return out.join("");
}

// Decompress an LZW-encoded string
exports.lzw_decode = function(s)
{
	var dict = {};
	var data = (s + "").split("");
	var currChar = data[0];
	var oldPhrase = currChar;
	var out = [currChar];
	var code = 256;
	var phrase;
	for (var i=1; i<data.length; i++) {
		var currCode = data[i].charCodeAt(0);
		if (currCode < 256) {
			phrase = data[i];
		}
		else {
			phrase = dict[currCode] ? dict[currCode] : (oldPhrase + currChar);
		}
		out.push(phrase);
		currChar = phrase.charAt(0);
		dict[code] = oldPhrase + currChar;
		code++;
		oldPhrase = phrase;
	}
	return out.join("");
};