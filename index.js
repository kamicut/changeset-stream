var K = require('kefir');
var request = require('request-promise');
var R = require('ramda');
var zlib = require('zlib');
var expat = require('node-expat');

var base_url = 'http://planet.osm.org/replication/changesets';
var state_url = base_url + '/state.yaml';

/* Takes a number representing the state and
 * outputs in the format xxx/xxx/xxx with zero
 * padding on the left
 * Number -> String
 */
function toDirectory (stateNumber) {
  // Pad with zeroes
  var stateStr = stateNumber.toString();
  var numZeros = 9 - stateStr.length;
  var zeros = R.repeat(0, numZeros);
  var pipe = R.pipe(
    R.split(''),
    R.concat(zeros),
    R.join(''),
    R.splitEvery(3),
    R.join('/'));
  return pipe(stateStr);
}

/* Converts osm changeset file to a stream
 * of JSON objects
 * String -> Stream JSON
 */
function MetaParser (xmlData) {
  var xmlParser = new expat.Parser('UTF-8');
  var _tempAttrs = {};
  return K.stream(function (emitter) {
    function endTag (symbol, attrs) {
      if (symbol === 'changeset') {
        emitter.emit(new Buffer(JSON.stringify(_tempAttrs) + '\n'), 'utf8');
      }
    }

    function startTag (symbol, attrs) {
      if (symbol === 'changeset') {
        if (attrs) {
          _tempAttrs = attrs;
        }
      }
      if (symbol === 'tag' && _tempAttrs && _tempAttrs.open === 'false') {
        _tempAttrs[attrs.k] = attrs.v;
      }
    }

    xmlParser.on('startElement', startTag);
    xmlParser.on('endElement', endTag);
    xmlParser.on('error', emitter.error);
    xmlParser.on('end', emitter.end);

    xmlParser.write(xmlData);
  });
}

/* Maps a state text file to a state number
 * String -> Number
 */
function getState (stateTextFile) {
  return Number(stateTextFile.substr(stateTextFile.length - 8));
}

// State property
var state = K.fromPoll(5000, function () {
    return K.fromPromise(request(state_url));
  })
  .flatMap()
  .map(getState)
  .skipDuplicates()
  .toProperty(R.always(0));

// Stream of URls
var urlStrings = state
  .changes()
  .map(toDirectory)
  .map(function (x) { return base_url + '/' + x + '.osm.gz'; });

// Stream of changeset XML data
var xmlData = urlStrings.flatMap(function (x) {
    return K.fromPromise(request({encoding: null, uri: x}));
  })
  .map(zlib.unzipSync)
  .map(R.toString);

// Stream of JSON objects
var parsedData = xmlData.flatMap(MetaParser).map(R.toString);

parsedData.onValue(console.log);
