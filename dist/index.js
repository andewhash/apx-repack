/******/ (() => { // webpackBootstrap
/******/ 	var __webpack_modules__ = ({

/***/ 560:
/***/ ((__unused_webpack_module, exports, __nccwpck_require__) => {

;(function (sax) { // wrapper for non-node envs
  sax.parser = function (strict, opt) { return new SAXParser(strict, opt) }
  sax.SAXParser = SAXParser
  sax.SAXStream = SAXStream
  sax.createStream = createStream

  // When we pass the MAX_BUFFER_LENGTH position, start checking for buffer overruns.
  // When we check, schedule the next check for MAX_BUFFER_LENGTH - (max(buffer lengths)),
  // since that's the earliest that a buffer overrun could occur.  This way, checks are
  // as rare as required, but as often as necessary to ensure never crossing this bound.
  // Furthermore, buffers are only tested at most once per write(), so passing a very
  // large string into write() might have undesirable effects, but this is manageable by
  // the caller, so it is assumed to be safe.  Thus, a call to write() may, in the extreme
  // edge case, result in creating at most one complete copy of the string passed in.
  // Set to Infinity to have unlimited buffers.
  sax.MAX_BUFFER_LENGTH = 64 * 1024

  var buffers = [
    'comment', 'sgmlDecl', 'textNode', 'tagName', 'doctype',
    'procInstName', 'procInstBody', 'entity', 'attribName',
    'attribValue', 'cdata', 'script'
  ]

  sax.EVENTS = [
    'text',
    'processinginstruction',
    'sgmldeclaration',
    'doctype',
    'comment',
    'opentagstart',
    'attribute',
    'opentag',
    'closetag',
    'opencdata',
    'cdata',
    'closecdata',
    'error',
    'end',
    'ready',
    'script',
    'opennamespace',
    'closenamespace'
  ]

  function SAXParser (strict, opt) {
    if (!(this instanceof SAXParser)) {
      return new SAXParser(strict, opt)
    }

    var parser = this
    clearBuffers(parser)
    parser.q = parser.c = ''
    parser.bufferCheckPosition = sax.MAX_BUFFER_LENGTH
    parser.opt = opt || {}
    parser.opt.lowercase = parser.opt.lowercase || parser.opt.lowercasetags
    parser.looseCase = parser.opt.lowercase ? 'toLowerCase' : 'toUpperCase'
    parser.tags = []
    parser.closed = parser.closedRoot = parser.sawRoot = false
    parser.tag = parser.error = null
    parser.strict = !!strict
    parser.noscript = !!(strict || parser.opt.noscript)
    parser.state = S.BEGIN
    parser.strictEntities = parser.opt.strictEntities
    parser.ENTITIES = parser.strictEntities ? Object.create(sax.XML_ENTITIES) : Object.create(sax.ENTITIES)
    parser.attribList = []

    // namespaces form a prototype chain.
    // it always points at the current tag,
    // which protos to its parent tag.
    if (parser.opt.xmlns) {
      parser.ns = Object.create(rootNS)
    }

    // disallow unquoted attribute values if not otherwise configured
    // and strict mode is true
    if (parser.opt.unquotedAttributeValues === undefined) {
      parser.opt.unquotedAttributeValues = !strict;
    }

    // mostly just for error reporting
    parser.trackPosition = parser.opt.position !== false
    if (parser.trackPosition) {
      parser.position = parser.line = parser.column = 0
    }
    emit(parser, 'onready')
  }

  if (!Object.create) {
    Object.create = function (o) {
      function F () {}
      F.prototype = o
      var newf = new F()
      return newf
    }
  }

  if (!Object.keys) {
    Object.keys = function (o) {
      var a = []
      for (var i in o) if (o.hasOwnProperty(i)) a.push(i)
      return a
    }
  }

  function checkBufferLength (parser) {
    var maxAllowed = Math.max(sax.MAX_BUFFER_LENGTH, 10)
    var maxActual = 0
    for (var i = 0, l = buffers.length; i < l; i++) {
      var len = parser[buffers[i]].length
      if (len > maxAllowed) {
        // Text/cdata nodes can get big, and since they're buffered,
        // we can get here under normal conditions.
        // Avoid issues by emitting the text node now,
        // so at least it won't get any bigger.
        switch (buffers[i]) {
          case 'textNode':
            closeText(parser)
            break

          case 'cdata':
            emitNode(parser, 'oncdata', parser.cdata)
            parser.cdata = ''
            break

          case 'script':
            emitNode(parser, 'onscript', parser.script)
            parser.script = ''
            break

          default:
            error(parser, 'Max buffer length exceeded: ' + buffers[i])
        }
      }
      maxActual = Math.max(maxActual, len)
    }
    // schedule the next check for the earliest possible buffer overrun.
    var m = sax.MAX_BUFFER_LENGTH - maxActual
    parser.bufferCheckPosition = m + parser.position
  }

  function clearBuffers (parser) {
    for (var i = 0, l = buffers.length; i < l; i++) {
      parser[buffers[i]] = ''
    }
  }

  function flushBuffers (parser) {
    closeText(parser)
    if (parser.cdata !== '') {
      emitNode(parser, 'oncdata', parser.cdata)
      parser.cdata = ''
    }
    if (parser.script !== '') {
      emitNode(parser, 'onscript', parser.script)
      parser.script = ''
    }
  }

  SAXParser.prototype = {
    end: function () { end(this) },
    write: write,
    resume: function () { this.error = null; return this },
    close: function () { return this.write(null) },
    flush: function () { flushBuffers(this) }
  }

  var Stream
  try {
    Stream = (__nccwpck_require__(203).Stream)
  } catch (ex) {
    Stream = function () {}
  }
  if (!Stream) Stream = function () {}

  var streamWraps = sax.EVENTS.filter(function (ev) {
    return ev !== 'error' && ev !== 'end'
  })

  function createStream (strict, opt) {
    return new SAXStream(strict, opt)
  }

  function SAXStream (strict, opt) {
    if (!(this instanceof SAXStream)) {
      return new SAXStream(strict, opt)
    }

    Stream.apply(this)

    this._parser = new SAXParser(strict, opt)
    this.writable = true
    this.readable = true

    var me = this

    this._parser.onend = function () {
      me.emit('end')
    }

    this._parser.onerror = function (er) {
      me.emit('error', er)

      // if didn't throw, then means error was handled.
      // go ahead and clear error, so we can write again.
      me._parser.error = null
    }

    this._decoder = null

    streamWraps.forEach(function (ev) {
      Object.defineProperty(me, 'on' + ev, {
        get: function () {
          return me._parser['on' + ev]
        },
        set: function (h) {
          if (!h) {
            me.removeAllListeners(ev)
            me._parser['on' + ev] = h
            return h
          }
          me.on(ev, h)
        },
        enumerable: true,
        configurable: false
      })
    })
  }

  SAXStream.prototype = Object.create(Stream.prototype, {
    constructor: {
      value: SAXStream
    }
  })

  SAXStream.prototype.write = function (data) {
    if (typeof Buffer === 'function' &&
      typeof Buffer.isBuffer === 'function' &&
      Buffer.isBuffer(data)) {
      if (!this._decoder) {
        var SD = (__nccwpck_require__(193).StringDecoder)
        this._decoder = new SD('utf8')
      }
      data = this._decoder.write(data)
    }

    this._parser.write(data.toString())
    this.emit('data', data)
    return true
  }

  SAXStream.prototype.end = function (chunk) {
    if (chunk && chunk.length) {
      this.write(chunk)
    }
    this._parser.end()
    return true
  }

  SAXStream.prototype.on = function (ev, handler) {
    var me = this
    if (!me._parser['on' + ev] && streamWraps.indexOf(ev) !== -1) {
      me._parser['on' + ev] = function () {
        var args = arguments.length === 1 ? [arguments[0]] : Array.apply(null, arguments)
        args.splice(0, 0, ev)
        me.emit.apply(me, args)
      }
    }

    return Stream.prototype.on.call(me, ev, handler)
  }

  // this really needs to be replaced with character classes.
  // XML allows all manner of ridiculous numbers and digits.
  var CDATA = '[CDATA['
  var DOCTYPE = 'DOCTYPE'
  var XML_NAMESPACE = 'http://www.w3.org/XML/1998/namespace'
  var XMLNS_NAMESPACE = 'http://www.w3.org/2000/xmlns/'
  var rootNS = { xml: XML_NAMESPACE, xmlns: XMLNS_NAMESPACE }

  // http://www.w3.org/TR/REC-xml/#NT-NameStartChar
  // This implementation works on strings, a single character at a time
  // as such, it cannot ever support astral-plane characters (10000-EFFFF)
  // without a significant breaking change to either this  parser, or the
  // JavaScript language.  Implementation of an emoji-capable xml parser
  // is left as an exercise for the reader.
  var nameStart = /[:_A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD]/

  var nameBody = /[:_A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\u00B7\u0300-\u036F\u203F-\u2040.\d-]/

  var entityStart = /[#:_A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD]/
  var entityBody = /[#:_A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\u00B7\u0300-\u036F\u203F-\u2040.\d-]/

  function isWhitespace (c) {
    return c === ' ' || c === '\n' || c === '\r' || c === '\t'
  }

  function isQuote (c) {
    return c === '"' || c === '\''
  }

  function isAttribEnd (c) {
    return c === '>' || isWhitespace(c)
  }

  function isMatch (regex, c) {
    return regex.test(c)
  }

  function notMatch (regex, c) {
    return !isMatch(regex, c)
  }

  var S = 0
  sax.STATE = {
    BEGIN: S++, // leading byte order mark or whitespace
    BEGIN_WHITESPACE: S++, // leading whitespace
    TEXT: S++, // general stuff
    TEXT_ENTITY: S++, // &amp and such.
    OPEN_WAKA: S++, // <
    SGML_DECL: S++, // <!BLARG
    SGML_DECL_QUOTED: S++, // <!BLARG foo "bar
    DOCTYPE: S++, // <!DOCTYPE
    DOCTYPE_QUOTED: S++, // <!DOCTYPE "//blah
    DOCTYPE_DTD: S++, // <!DOCTYPE "//blah" [ ...
    DOCTYPE_DTD_QUOTED: S++, // <!DOCTYPE "//blah" [ "foo
    COMMENT_STARTING: S++, // <!-
    COMMENT: S++, // <!--
    COMMENT_ENDING: S++, // <!-- blah -
    COMMENT_ENDED: S++, // <!-- blah --
    CDATA: S++, // <![CDATA[ something
    CDATA_ENDING: S++, // ]
    CDATA_ENDING_2: S++, // ]]
    PROC_INST: S++, // <?hi
    PROC_INST_BODY: S++, // <?hi there
    PROC_INST_ENDING: S++, // <?hi "there" ?
    OPEN_TAG: S++, // <strong
    OPEN_TAG_SLASH: S++, // <strong /
    ATTRIB: S++, // <a
    ATTRIB_NAME: S++, // <a foo
    ATTRIB_NAME_SAW_WHITE: S++, // <a foo _
    ATTRIB_VALUE: S++, // <a foo=
    ATTRIB_VALUE_QUOTED: S++, // <a foo="bar
    ATTRIB_VALUE_CLOSED: S++, // <a foo="bar"
    ATTRIB_VALUE_UNQUOTED: S++, // <a foo=bar
    ATTRIB_VALUE_ENTITY_Q: S++, // <foo bar="&quot;"
    ATTRIB_VALUE_ENTITY_U: S++, // <foo bar=&quot
    CLOSE_TAG: S++, // </a
    CLOSE_TAG_SAW_WHITE: S++, // </a   >
    SCRIPT: S++, // <script> ...
    SCRIPT_ENDING: S++ // <script> ... <
  }

  sax.XML_ENTITIES = {
    'amp': '&',
    'gt': '>',
    'lt': '<',
    'quot': '"',
    'apos': "'"
  }

  sax.ENTITIES = {
    'amp': '&',
    'gt': '>',
    'lt': '<',
    'quot': '"',
    'apos': "'",
    'AElig': 198,
    'Aacute': 193,
    'Acirc': 194,
    'Agrave': 192,
    'Aring': 197,
    'Atilde': 195,
    'Auml': 196,
    'Ccedil': 199,
    'ETH': 208,
    'Eacute': 201,
    'Ecirc': 202,
    'Egrave': 200,
    'Euml': 203,
    'Iacute': 205,
    'Icirc': 206,
    'Igrave': 204,
    'Iuml': 207,
    'Ntilde': 209,
    'Oacute': 211,
    'Ocirc': 212,
    'Ograve': 210,
    'Oslash': 216,
    'Otilde': 213,
    'Ouml': 214,
    'THORN': 222,
    'Uacute': 218,
    'Ucirc': 219,
    'Ugrave': 217,
    'Uuml': 220,
    'Yacute': 221,
    'aacute': 225,
    'acirc': 226,
    'aelig': 230,
    'agrave': 224,
    'aring': 229,
    'atilde': 227,
    'auml': 228,
    'ccedil': 231,
    'eacute': 233,
    'ecirc': 234,
    'egrave': 232,
    'eth': 240,
    'euml': 235,
    'iacute': 237,
    'icirc': 238,
    'igrave': 236,
    'iuml': 239,
    'ntilde': 241,
    'oacute': 243,
    'ocirc': 244,
    'ograve': 242,
    'oslash': 248,
    'otilde': 245,
    'ouml': 246,
    'szlig': 223,
    'thorn': 254,
    'uacute': 250,
    'ucirc': 251,
    'ugrave': 249,
    'uuml': 252,
    'yacute': 253,
    'yuml': 255,
    'copy': 169,
    'reg': 174,
    'nbsp': 160,
    'iexcl': 161,
    'cent': 162,
    'pound': 163,
    'curren': 164,
    'yen': 165,
    'brvbar': 166,
    'sect': 167,
    'uml': 168,
    'ordf': 170,
    'laquo': 171,
    'not': 172,
    'shy': 173,
    'macr': 175,
    'deg': 176,
    'plusmn': 177,
    'sup1': 185,
    'sup2': 178,
    'sup3': 179,
    'acute': 180,
    'micro': 181,
    'para': 182,
    'middot': 183,
    'cedil': 184,
    'ordm': 186,
    'raquo': 187,
    'frac14': 188,
    'frac12': 189,
    'frac34': 190,
    'iquest': 191,
    'times': 215,
    'divide': 247,
    'OElig': 338,
    'oelig': 339,
    'Scaron': 352,
    'scaron': 353,
    'Yuml': 376,
    'fnof': 402,
    'circ': 710,
    'tilde': 732,
    'Alpha': 913,
    'Beta': 914,
    'Gamma': 915,
    'Delta': 916,
    'Epsilon': 917,
    'Zeta': 918,
    'Eta': 919,
    'Theta': 920,
    'Iota': 921,
    'Kappa': 922,
    'Lambda': 923,
    'Mu': 924,
    'Nu': 925,
    'Xi': 926,
    'Omicron': 927,
    'Pi': 928,
    'Rho': 929,
    'Sigma': 931,
    'Tau': 932,
    'Upsilon': 933,
    'Phi': 934,
    'Chi': 935,
    'Psi': 936,
    'Omega': 937,
    'alpha': 945,
    'beta': 946,
    'gamma': 947,
    'delta': 948,
    'epsilon': 949,
    'zeta': 950,
    'eta': 951,
    'theta': 952,
    'iota': 953,
    'kappa': 954,
    'lambda': 955,
    'mu': 956,
    'nu': 957,
    'xi': 958,
    'omicron': 959,
    'pi': 960,
    'rho': 961,
    'sigmaf': 962,
    'sigma': 963,
    'tau': 964,
    'upsilon': 965,
    'phi': 966,
    'chi': 967,
    'psi': 968,
    'omega': 969,
    'thetasym': 977,
    'upsih': 978,
    'piv': 982,
    'ensp': 8194,
    'emsp': 8195,
    'thinsp': 8201,
    'zwnj': 8204,
    'zwj': 8205,
    'lrm': 8206,
    'rlm': 8207,
    'ndash': 8211,
    'mdash': 8212,
    'lsquo': 8216,
    'rsquo': 8217,
    'sbquo': 8218,
    'ldquo': 8220,
    'rdquo': 8221,
    'bdquo': 8222,
    'dagger': 8224,
    'Dagger': 8225,
    'bull': 8226,
    'hellip': 8230,
    'permil': 8240,
    'prime': 8242,
    'Prime': 8243,
    'lsaquo': 8249,
    'rsaquo': 8250,
    'oline': 8254,
    'frasl': 8260,
    'euro': 8364,
    'image': 8465,
    'weierp': 8472,
    'real': 8476,
    'trade': 8482,
    'alefsym': 8501,
    'larr': 8592,
    'uarr': 8593,
    'rarr': 8594,
    'darr': 8595,
    'harr': 8596,
    'crarr': 8629,
    'lArr': 8656,
    'uArr': 8657,
    'rArr': 8658,
    'dArr': 8659,
    'hArr': 8660,
    'forall': 8704,
    'part': 8706,
    'exist': 8707,
    'empty': 8709,
    'nabla': 8711,
    'isin': 8712,
    'notin': 8713,
    'ni': 8715,
    'prod': 8719,
    'sum': 8721,
    'minus': 8722,
    'lowast': 8727,
    'radic': 8730,
    'prop': 8733,
    'infin': 8734,
    'ang': 8736,
    'and': 8743,
    'or': 8744,
    'cap': 8745,
    'cup': 8746,
    'int': 8747,
    'there4': 8756,
    'sim': 8764,
    'cong': 8773,
    'asymp': 8776,
    'ne': 8800,
    'equiv': 8801,
    'le': 8804,
    'ge': 8805,
    'sub': 8834,
    'sup': 8835,
    'nsub': 8836,
    'sube': 8838,
    'supe': 8839,
    'oplus': 8853,
    'otimes': 8855,
    'perp': 8869,
    'sdot': 8901,
    'lceil': 8968,
    'rceil': 8969,
    'lfloor': 8970,
    'rfloor': 8971,
    'lang': 9001,
    'rang': 9002,
    'loz': 9674,
    'spades': 9824,
    'clubs': 9827,
    'hearts': 9829,
    'diams': 9830
  }

  Object.keys(sax.ENTITIES).forEach(function (key) {
    var e = sax.ENTITIES[key]
    var s = typeof e === 'number' ? String.fromCharCode(e) : e
    sax.ENTITIES[key] = s
  })

  for (var s in sax.STATE) {
    sax.STATE[sax.STATE[s]] = s
  }

  // shorthand
  S = sax.STATE

  function emit (parser, event, data) {
    parser[event] && parser[event](data)
  }

  function emitNode (parser, nodeType, data) {
    if (parser.textNode) closeText(parser)
    emit(parser, nodeType, data)
  }

  function closeText (parser) {
    parser.textNode = textopts(parser.opt, parser.textNode)
    if (parser.textNode) emit(parser, 'ontext', parser.textNode)
    parser.textNode = ''
  }

  function textopts (opt, text) {
    if (opt.trim) text = text.trim()
    if (opt.normalize) text = text.replace(/\s+/g, ' ')
    return text
  }

  function error (parser, er) {
    closeText(parser)
    if (parser.trackPosition) {
      er += '\nLine: ' + parser.line +
        '\nColumn: ' + parser.column +
        '\nChar: ' + parser.c
    }
    er = new Error(er)
    parser.error = er
    emit(parser, 'onerror', er)
    return parser
  }

  function end (parser) {
    if (parser.sawRoot && !parser.closedRoot) strictFail(parser, 'Unclosed root tag')
    if ((parser.state !== S.BEGIN) &&
      (parser.state !== S.BEGIN_WHITESPACE) &&
      (parser.state !== S.TEXT)) {
      error(parser, 'Unexpected end')
    }
    closeText(parser)
    parser.c = ''
    parser.closed = true
    emit(parser, 'onend')
    SAXParser.call(parser, parser.strict, parser.opt)
    return parser
  }

  function strictFail (parser, message) {
    if (typeof parser !== 'object' || !(parser instanceof SAXParser)) {
      throw new Error('bad call to strictFail')
    }
    if (parser.strict) {
      error(parser, message)
    }
  }

  function newTag (parser) {
    if (!parser.strict) parser.tagName = parser.tagName[parser.looseCase]()
    var parent = parser.tags[parser.tags.length - 1] || parser
    var tag = parser.tag = { name: parser.tagName, attributes: {} }

    // will be overridden if tag contails an xmlns="foo" or xmlns:foo="bar"
    if (parser.opt.xmlns) {
      tag.ns = parent.ns
    }
    parser.attribList.length = 0
    emitNode(parser, 'onopentagstart', tag)
  }

  function qname (name, attribute) {
    var i = name.indexOf(':')
    var qualName = i < 0 ? [ '', name ] : name.split(':')
    var prefix = qualName[0]
    var local = qualName[1]

    // <x "xmlns"="http://foo">
    if (attribute && name === 'xmlns') {
      prefix = 'xmlns'
      local = ''
    }

    return { prefix: prefix, local: local }
  }

  function attrib (parser) {
    if (!parser.strict) {
      parser.attribName = parser.attribName[parser.looseCase]()
    }

    if (parser.attribList.indexOf(parser.attribName) !== -1 ||
      parser.tag.attributes.hasOwnProperty(parser.attribName)) {
      parser.attribName = parser.attribValue = ''
      return
    }

    if (parser.opt.xmlns) {
      var qn = qname(parser.attribName, true)
      var prefix = qn.prefix
      var local = qn.local

      if (prefix === 'xmlns') {
        // namespace binding attribute. push the binding into scope
        if (local === 'xml' && parser.attribValue !== XML_NAMESPACE) {
          strictFail(parser,
            'xml: prefix must be bound to ' + XML_NAMESPACE + '\n' +
            'Actual: ' + parser.attribValue)
        } else if (local === 'xmlns' && parser.attribValue !== XMLNS_NAMESPACE) {
          strictFail(parser,
            'xmlns: prefix must be bound to ' + XMLNS_NAMESPACE + '\n' +
            'Actual: ' + parser.attribValue)
        } else {
          var tag = parser.tag
          var parent = parser.tags[parser.tags.length - 1] || parser
          if (tag.ns === parent.ns) {
            tag.ns = Object.create(parent.ns)
          }
          tag.ns[local] = parser.attribValue
        }
      }

      // defer onattribute events until all attributes have been seen
      // so any new bindings can take effect. preserve attribute order
      // so deferred events can be emitted in document order
      parser.attribList.push([parser.attribName, parser.attribValue])
    } else {
      // in non-xmlns mode, we can emit the event right away
      parser.tag.attributes[parser.attribName] = parser.attribValue
      emitNode(parser, 'onattribute', {
        name: parser.attribName,
        value: parser.attribValue
      })
    }

    parser.attribName = parser.attribValue = ''
  }

  function openTag (parser, selfClosing) {
    if (parser.opt.xmlns) {
      // emit namespace binding events
      var tag = parser.tag

      // add namespace info to tag
      var qn = qname(parser.tagName)
      tag.prefix = qn.prefix
      tag.local = qn.local
      tag.uri = tag.ns[qn.prefix] || ''

      if (tag.prefix && !tag.uri) {
        strictFail(parser, 'Unbound namespace prefix: ' +
          JSON.stringify(parser.tagName))
        tag.uri = qn.prefix
      }

      var parent = parser.tags[parser.tags.length - 1] || parser
      if (tag.ns && parent.ns !== tag.ns) {
        Object.keys(tag.ns).forEach(function (p) {
          emitNode(parser, 'onopennamespace', {
            prefix: p,
            uri: tag.ns[p]
          })
        })
      }

      // handle deferred onattribute events
      // Note: do not apply default ns to attributes:
      //   http://www.w3.org/TR/REC-xml-names/#defaulting
      for (var i = 0, l = parser.attribList.length; i < l; i++) {
        var nv = parser.attribList[i]
        var name = nv[0]
        var value = nv[1]
        var qualName = qname(name, true)
        var prefix = qualName.prefix
        var local = qualName.local
        var uri = prefix === '' ? '' : (tag.ns[prefix] || '')
        var a = {
          name: name,
          value: value,
          prefix: prefix,
          local: local,
          uri: uri
        }

        // if there's any attributes with an undefined namespace,
        // then fail on them now.
        if (prefix && prefix !== 'xmlns' && !uri) {
          strictFail(parser, 'Unbound namespace prefix: ' +
            JSON.stringify(prefix))
          a.uri = prefix
        }
        parser.tag.attributes[name] = a
        emitNode(parser, 'onattribute', a)
      }
      parser.attribList.length = 0
    }

    parser.tag.isSelfClosing = !!selfClosing

    // process the tag
    parser.sawRoot = true
    parser.tags.push(parser.tag)
    emitNode(parser, 'onopentag', parser.tag)
    if (!selfClosing) {
      // special case for <script> in non-strict mode.
      if (!parser.noscript && parser.tagName.toLowerCase() === 'script') {
        parser.state = S.SCRIPT
      } else {
        parser.state = S.TEXT
      }
      parser.tag = null
      parser.tagName = ''
    }
    parser.attribName = parser.attribValue = ''
    parser.attribList.length = 0
  }

  function closeTag (parser) {
    if (!parser.tagName) {
      strictFail(parser, 'Weird empty close tag.')
      parser.textNode += '</>'
      parser.state = S.TEXT
      return
    }

    if (parser.script) {
      if (parser.tagName !== 'script') {
        parser.script += '</' + parser.tagName + '>'
        parser.tagName = ''
        parser.state = S.SCRIPT
        return
      }
      emitNode(parser, 'onscript', parser.script)
      parser.script = ''
    }

    // first make sure that the closing tag actually exists.
    // <a><b></c></b></a> will close everything, otherwise.
    var t = parser.tags.length
    var tagName = parser.tagName
    if (!parser.strict) {
      tagName = tagName[parser.looseCase]()
    }
    var closeTo = tagName
    while (t--) {
      var close = parser.tags[t]
      if (close.name !== closeTo) {
        // fail the first time in strict mode
        strictFail(parser, 'Unexpected close tag')
      } else {
        break
      }
    }

    // didn't find it.  we already failed for strict, so just abort.
    if (t < 0) {
      strictFail(parser, 'Unmatched closing tag: ' + parser.tagName)
      parser.textNode += '</' + parser.tagName + '>'
      parser.state = S.TEXT
      return
    }
    parser.tagName = tagName
    var s = parser.tags.length
    while (s-- > t) {
      var tag = parser.tag = parser.tags.pop()
      parser.tagName = parser.tag.name
      emitNode(parser, 'onclosetag', parser.tagName)

      var x = {}
      for (var i in tag.ns) {
        x[i] = tag.ns[i]
      }

      var parent = parser.tags[parser.tags.length - 1] || parser
      if (parser.opt.xmlns && tag.ns !== parent.ns) {
        // remove namespace bindings introduced by tag
        Object.keys(tag.ns).forEach(function (p) {
          var n = tag.ns[p]
          emitNode(parser, 'onclosenamespace', { prefix: p, uri: n })
        })
      }
    }
    if (t === 0) parser.closedRoot = true
    parser.tagName = parser.attribValue = parser.attribName = ''
    parser.attribList.length = 0
    parser.state = S.TEXT
  }

  function parseEntity (parser) {
    var entity = parser.entity
    var entityLC = entity.toLowerCase()
    var num
    var numStr = ''

    if (parser.ENTITIES[entity]) {
      return parser.ENTITIES[entity]
    }
    if (parser.ENTITIES[entityLC]) {
      return parser.ENTITIES[entityLC]
    }
    entity = entityLC
    if (entity.charAt(0) === '#') {
      if (entity.charAt(1) === 'x') {
        entity = entity.slice(2)
        num = parseInt(entity, 16)
        numStr = num.toString(16)
      } else {
        entity = entity.slice(1)
        num = parseInt(entity, 10)
        numStr = num.toString(10)
      }
    }
    entity = entity.replace(/^0+/, '')
    if (isNaN(num) || numStr.toLowerCase() !== entity) {
      strictFail(parser, 'Invalid character entity')
      return '&' + parser.entity + ';'
    }

    return String.fromCodePoint(num)
  }

  function beginWhiteSpace (parser, c) {
    if (c === '<') {
      parser.state = S.OPEN_WAKA
      parser.startTagPosition = parser.position
    } else if (!isWhitespace(c)) {
      // have to process this as a text node.
      // weird, but happens.
      strictFail(parser, 'Non-whitespace before first tag.')
      parser.textNode = c
      parser.state = S.TEXT
    }
  }

  function charAt (chunk, i) {
    var result = ''
    if (i < chunk.length) {
      result = chunk.charAt(i)
    }
    return result
  }

  function write (chunk) {
    var parser = this
    if (this.error) {
      throw this.error
    }
    if (parser.closed) {
      return error(parser,
        'Cannot write after close. Assign an onready handler.')
    }
    if (chunk === null) {
      return end(parser)
    }
    if (typeof chunk === 'object') {
      chunk = chunk.toString()
    }
    var i = 0
    var c = ''
    while (true) {
      c = charAt(chunk, i++)
      parser.c = c

      if (!c) {
        break
      }

      if (parser.trackPosition) {
        parser.position++
        if (c === '\n') {
          parser.line++
          parser.column = 0
        } else {
          parser.column++
        }
      }

      switch (parser.state) {
        case S.BEGIN:
          parser.state = S.BEGIN_WHITESPACE
          if (c === '\uFEFF') {
            continue
          }
          beginWhiteSpace(parser, c)
          continue

        case S.BEGIN_WHITESPACE:
          beginWhiteSpace(parser, c)
          continue

        case S.TEXT:
          if (parser.sawRoot && !parser.closedRoot) {
            var starti = i - 1
            while (c && c !== '<' && c !== '&') {
              c = charAt(chunk, i++)
              if (c && parser.trackPosition) {
                parser.position++
                if (c === '\n') {
                  parser.line++
                  parser.column = 0
                } else {
                  parser.column++
                }
              }
            }
            parser.textNode += chunk.substring(starti, i - 1)
          }
          if (c === '<' && !(parser.sawRoot && parser.closedRoot && !parser.strict)) {
            parser.state = S.OPEN_WAKA
            parser.startTagPosition = parser.position
          } else {
            if (!isWhitespace(c) && (!parser.sawRoot || parser.closedRoot)) {
              strictFail(parser, 'Text data outside of root node.')
            }
            if (c === '&') {
              parser.state = S.TEXT_ENTITY
            } else {
              parser.textNode += c
            }
          }
          continue

        case S.SCRIPT:
          // only non-strict
          if (c === '<') {
            parser.state = S.SCRIPT_ENDING
          } else {
            parser.script += c
          }
          continue

        case S.SCRIPT_ENDING:
          if (c === '/') {
            parser.state = S.CLOSE_TAG
          } else {
            parser.script += '<' + c
            parser.state = S.SCRIPT
          }
          continue

        case S.OPEN_WAKA:
          // either a /, ?, !, or text is coming next.
          if (c === '!') {
            parser.state = S.SGML_DECL
            parser.sgmlDecl = ''
          } else if (isWhitespace(c)) {
            // wait for it...
          } else if (isMatch(nameStart, c)) {
            parser.state = S.OPEN_TAG
            parser.tagName = c
          } else if (c === '/') {
            parser.state = S.CLOSE_TAG
            parser.tagName = ''
          } else if (c === '?') {
            parser.state = S.PROC_INST
            parser.procInstName = parser.procInstBody = ''
          } else {
            strictFail(parser, 'Unencoded <')
            // if there was some whitespace, then add that in.
            if (parser.startTagPosition + 1 < parser.position) {
              var pad = parser.position - parser.startTagPosition
              c = new Array(pad).join(' ') + c
            }
            parser.textNode += '<' + c
            parser.state = S.TEXT
          }
          continue

        case S.SGML_DECL:
          if (parser.sgmlDecl + c === '--') {
            parser.state = S.COMMENT
            parser.comment = ''
            parser.sgmlDecl = ''
            continue;
          }

          if (parser.doctype && parser.doctype !== true && parser.sgmlDecl) {
            parser.state = S.DOCTYPE_DTD
            parser.doctype += '<!' + parser.sgmlDecl + c
            parser.sgmlDecl = ''
          } else if ((parser.sgmlDecl + c).toUpperCase() === CDATA) {
            emitNode(parser, 'onopencdata')
            parser.state = S.CDATA
            parser.sgmlDecl = ''
            parser.cdata = ''
          } else if ((parser.sgmlDecl + c).toUpperCase() === DOCTYPE) {
            parser.state = S.DOCTYPE
            if (parser.doctype || parser.sawRoot) {
              strictFail(parser,
                'Inappropriately located doctype declaration')
            }
            parser.doctype = ''
            parser.sgmlDecl = ''
          } else if (c === '>') {
            emitNode(parser, 'onsgmldeclaration', parser.sgmlDecl)
            parser.sgmlDecl = ''
            parser.state = S.TEXT
          } else if (isQuote(c)) {
            parser.state = S.SGML_DECL_QUOTED
            parser.sgmlDecl += c
          } else {
            parser.sgmlDecl += c
          }
          continue

        case S.SGML_DECL_QUOTED:
          if (c === parser.q) {
            parser.state = S.SGML_DECL
            parser.q = ''
          }
          parser.sgmlDecl += c
          continue

        case S.DOCTYPE:
          if (c === '>') {
            parser.state = S.TEXT
            emitNode(parser, 'ondoctype', parser.doctype)
            parser.doctype = true // just remember that we saw it.
          } else {
            parser.doctype += c
            if (c === '[') {
              parser.state = S.DOCTYPE_DTD
            } else if (isQuote(c)) {
              parser.state = S.DOCTYPE_QUOTED
              parser.q = c
            }
          }
          continue

        case S.DOCTYPE_QUOTED:
          parser.doctype += c
          if (c === parser.q) {
            parser.q = ''
            parser.state = S.DOCTYPE
          }
          continue

        case S.DOCTYPE_DTD:
          if (c === ']') {
            parser.doctype += c
            parser.state = S.DOCTYPE
          } else if (c === '<') {
            parser.state = S.OPEN_WAKA
            parser.startTagPosition = parser.position
          } else if (isQuote(c)) {
            parser.doctype += c
            parser.state = S.DOCTYPE_DTD_QUOTED
            parser.q = c
          } else {
            parser.doctype += c
          }
          continue

        case S.DOCTYPE_DTD_QUOTED:
          parser.doctype += c
          if (c === parser.q) {
            parser.state = S.DOCTYPE_DTD
            parser.q = ''
          }
          continue

        case S.COMMENT:
          if (c === '-') {
            parser.state = S.COMMENT_ENDING
          } else {
            parser.comment += c
          }
          continue

        case S.COMMENT_ENDING:
          if (c === '-') {
            parser.state = S.COMMENT_ENDED
            parser.comment = textopts(parser.opt, parser.comment)
            if (parser.comment) {
              emitNode(parser, 'oncomment', parser.comment)
            }
            parser.comment = ''
          } else {
            parser.comment += '-' + c
            parser.state = S.COMMENT
          }
          continue

        case S.COMMENT_ENDED:
          if (c !== '>') {
            strictFail(parser, 'Malformed comment')
            // allow <!-- blah -- bloo --> in non-strict mode,
            // which is a comment of " blah -- bloo "
            parser.comment += '--' + c
            parser.state = S.COMMENT
          } else if (parser.doctype && parser.doctype !== true) {
            parser.state = S.DOCTYPE_DTD
          } else {
            parser.state = S.TEXT
          }
          continue

        case S.CDATA:
          if (c === ']') {
            parser.state = S.CDATA_ENDING
          } else {
            parser.cdata += c
          }
          continue

        case S.CDATA_ENDING:
          if (c === ']') {
            parser.state = S.CDATA_ENDING_2
          } else {
            parser.cdata += ']' + c
            parser.state = S.CDATA
          }
          continue

        case S.CDATA_ENDING_2:
          if (c === '>') {
            if (parser.cdata) {
              emitNode(parser, 'oncdata', parser.cdata)
            }
            emitNode(parser, 'onclosecdata')
            parser.cdata = ''
            parser.state = S.TEXT
          } else if (c === ']') {
            parser.cdata += ']'
          } else {
            parser.cdata += ']]' + c
            parser.state = S.CDATA
          }
          continue

        case S.PROC_INST:
          if (c === '?') {
            parser.state = S.PROC_INST_ENDING
          } else if (isWhitespace(c)) {
            parser.state = S.PROC_INST_BODY
          } else {
            parser.procInstName += c
          }
          continue

        case S.PROC_INST_BODY:
          if (!parser.procInstBody && isWhitespace(c)) {
            continue
          } else if (c === '?') {
            parser.state = S.PROC_INST_ENDING
          } else {
            parser.procInstBody += c
          }
          continue

        case S.PROC_INST_ENDING:
          if (c === '>') {
            emitNode(parser, 'onprocessinginstruction', {
              name: parser.procInstName,
              body: parser.procInstBody
            })
            parser.procInstName = parser.procInstBody = ''
            parser.state = S.TEXT
          } else {
            parser.procInstBody += '?' + c
            parser.state = S.PROC_INST_BODY
          }
          continue

        case S.OPEN_TAG:
          if (isMatch(nameBody, c)) {
            parser.tagName += c
          } else {
            newTag(parser)
            if (c === '>') {
              openTag(parser)
            } else if (c === '/') {
              parser.state = S.OPEN_TAG_SLASH
            } else {
              if (!isWhitespace(c)) {
                strictFail(parser, 'Invalid character in tag name')
              }
              parser.state = S.ATTRIB
            }
          }
          continue

        case S.OPEN_TAG_SLASH:
          if (c === '>') {
            openTag(parser, true)
            closeTag(parser)
          } else {
            strictFail(parser, 'Forward-slash in opening tag not followed by >')
            parser.state = S.ATTRIB
          }
          continue

        case S.ATTRIB:
          // haven't read the attribute name yet.
          if (isWhitespace(c)) {
            continue
          } else if (c === '>') {
            openTag(parser)
          } else if (c === '/') {
            parser.state = S.OPEN_TAG_SLASH
          } else if (isMatch(nameStart, c)) {
            parser.attribName = c
            parser.attribValue = ''
            parser.state = S.ATTRIB_NAME
          } else {
            strictFail(parser, 'Invalid attribute name')
          }
          continue

        case S.ATTRIB_NAME:
          if (c === '=') {
            parser.state = S.ATTRIB_VALUE
          } else if (c === '>') {
            strictFail(parser, 'Attribute without value')
            parser.attribValue = parser.attribName
            attrib(parser)
            openTag(parser)
          } else if (isWhitespace(c)) {
            parser.state = S.ATTRIB_NAME_SAW_WHITE
          } else if (isMatch(nameBody, c)) {
            parser.attribName += c
          } else {
            strictFail(parser, 'Invalid attribute name')
          }
          continue

        case S.ATTRIB_NAME_SAW_WHITE:
          if (c === '=') {
            parser.state = S.ATTRIB_VALUE
          } else if (isWhitespace(c)) {
            continue
          } else {
            strictFail(parser, 'Attribute without value')
            parser.tag.attributes[parser.attribName] = ''
            parser.attribValue = ''
            emitNode(parser, 'onattribute', {
              name: parser.attribName,
              value: ''
            })
            parser.attribName = ''
            if (c === '>') {
              openTag(parser)
            } else if (isMatch(nameStart, c)) {
              parser.attribName = c
              parser.state = S.ATTRIB_NAME
            } else {
              strictFail(parser, 'Invalid attribute name')
              parser.state = S.ATTRIB
            }
          }
          continue

        case S.ATTRIB_VALUE:
          if (isWhitespace(c)) {
            continue
          } else if (isQuote(c)) {
            parser.q = c
            parser.state = S.ATTRIB_VALUE_QUOTED
          } else {
            if (!parser.opt.unquotedAttributeValues) {
              error(parser, 'Unquoted attribute value')
            }
            parser.state = S.ATTRIB_VALUE_UNQUOTED
            parser.attribValue = c
          }
          continue

        case S.ATTRIB_VALUE_QUOTED:
          if (c !== parser.q) {
            if (c === '&') {
              parser.state = S.ATTRIB_VALUE_ENTITY_Q
            } else {
              parser.attribValue += c
            }
            continue
          }
          attrib(parser)
          parser.q = ''
          parser.state = S.ATTRIB_VALUE_CLOSED
          continue

        case S.ATTRIB_VALUE_CLOSED:
          if (isWhitespace(c)) {
            parser.state = S.ATTRIB
          } else if (c === '>') {
            openTag(parser)
          } else if (c === '/') {
            parser.state = S.OPEN_TAG_SLASH
          } else if (isMatch(nameStart, c)) {
            strictFail(parser, 'No whitespace between attributes')
            parser.attribName = c
            parser.attribValue = ''
            parser.state = S.ATTRIB_NAME
          } else {
            strictFail(parser, 'Invalid attribute name')
          }
          continue

        case S.ATTRIB_VALUE_UNQUOTED:
          if (!isAttribEnd(c)) {
            if (c === '&') {
              parser.state = S.ATTRIB_VALUE_ENTITY_U
            } else {
              parser.attribValue += c
            }
            continue
          }
          attrib(parser)
          if (c === '>') {
            openTag(parser)
          } else {
            parser.state = S.ATTRIB
          }
          continue

        case S.CLOSE_TAG:
          if (!parser.tagName) {
            if (isWhitespace(c)) {
              continue
            } else if (notMatch(nameStart, c)) {
              if (parser.script) {
                parser.script += '</' + c
                parser.state = S.SCRIPT
              } else {
                strictFail(parser, 'Invalid tagname in closing tag.')
              }
            } else {
              parser.tagName = c
            }
          } else if (c === '>') {
            closeTag(parser)
          } else if (isMatch(nameBody, c)) {
            parser.tagName += c
          } else if (parser.script) {
            parser.script += '</' + parser.tagName
            parser.tagName = ''
            parser.state = S.SCRIPT
          } else {
            if (!isWhitespace(c)) {
              strictFail(parser, 'Invalid tagname in closing tag')
            }
            parser.state = S.CLOSE_TAG_SAW_WHITE
          }
          continue

        case S.CLOSE_TAG_SAW_WHITE:
          if (isWhitespace(c)) {
            continue
          }
          if (c === '>') {
            closeTag(parser)
          } else {
            strictFail(parser, 'Invalid characters in closing tag')
          }
          continue

        case S.TEXT_ENTITY:
        case S.ATTRIB_VALUE_ENTITY_Q:
        case S.ATTRIB_VALUE_ENTITY_U:
          var returnState
          var buffer
          switch (parser.state) {
            case S.TEXT_ENTITY:
              returnState = S.TEXT
              buffer = 'textNode'
              break

            case S.ATTRIB_VALUE_ENTITY_Q:
              returnState = S.ATTRIB_VALUE_QUOTED
              buffer = 'attribValue'
              break

            case S.ATTRIB_VALUE_ENTITY_U:
              returnState = S.ATTRIB_VALUE_UNQUOTED
              buffer = 'attribValue'
              break
          }

          if (c === ';') {
            var parsedEntity = parseEntity(parser)
            if (parser.opt.unparsedEntities && !Object.values(sax.XML_ENTITIES).includes(parsedEntity)) {
              parser.entity = ''
              parser.state = returnState
              parser.write(parsedEntity)
            } else {
              parser[buffer] += parsedEntity
              parser.entity = ''
              parser.state = returnState
            }
          } else if (isMatch(parser.entity.length ? entityBody : entityStart, c)) {
            parser.entity += c
          } else {
            strictFail(parser, 'Invalid character in entity name')
            parser[buffer] += '&' + parser.entity + c
            parser.entity = ''
            parser.state = returnState
          }

          continue

        default: /* istanbul ignore next */ {
          throw new Error(parser, 'Unknown state: ' + parser.state)
        }
      }
    } // while

    if (parser.position >= parser.bufferCheckPosition) {
      checkBufferLength(parser)
    }
    return parser
  }

  /*! http://mths.be/fromcodepoint v0.1.0 by @mathias */
  /* istanbul ignore next */
  if (!String.fromCodePoint) {
    (function () {
      var stringFromCharCode = String.fromCharCode
      var floor = Math.floor
      var fromCodePoint = function () {
        var MAX_SIZE = 0x4000
        var codeUnits = []
        var highSurrogate
        var lowSurrogate
        var index = -1
        var length = arguments.length
        if (!length) {
          return ''
        }
        var result = ''
        while (++index < length) {
          var codePoint = Number(arguments[index])
          if (
            !isFinite(codePoint) || // `NaN`, `+Infinity`, or `-Infinity`
            codePoint < 0 || // not a valid Unicode code point
            codePoint > 0x10FFFF || // not a valid Unicode code point
            floor(codePoint) !== codePoint // not an integer
          ) {
            throw RangeError('Invalid code point: ' + codePoint)
          }
          if (codePoint <= 0xFFFF) { // BMP code point
            codeUnits.push(codePoint)
          } else { // Astral code point; split in surrogate halves
            // http://mathiasbynens.be/notes/javascript-encoding#surrogate-formulae
            codePoint -= 0x10000
            highSurrogate = (codePoint >> 10) + 0xD800
            lowSurrogate = (codePoint % 0x400) + 0xDC00
            codeUnits.push(highSurrogate, lowSurrogate)
          }
          if (index + 1 === length || codeUnits.length > MAX_SIZE) {
            result += stringFromCharCode.apply(null, codeUnits)
            codeUnits.length = 0
          }
        }
        return result
      }
      /* istanbul ignore next */
      if (Object.defineProperty) {
        Object.defineProperty(String, 'fromCodePoint', {
          value: fromCodePoint,
          configurable: true,
          writable: true
        })
      } else {
        String.fromCodePoint = fromCodePoint
      }
    }())
  }
})( false ? 0 : exports)


/***/ }),

/***/ 405:
/***/ (function(__unused_webpack_module, exports, __nccwpck_require__) {

"use strict";

var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.runRepack = runRepack;
const fs = __importStar(__nccwpck_require__(896));
const path = __importStar(__nccwpck_require__(928));
const sniff_service_1 = __nccwpck_require__(521);
const factory_1 = __nccwpck_require__(210);
async function runRepack(params) {
    const { inFile, outFile, utcOffset = 0, includeJso = false } = params;
    if (!fs.existsSync(inFile)) {
        throw new Error(`No such file: ${inFile}`);
    }
    const absIn = path.resolve(inFile);
    const absOut = path.resolve(outFile);
    const kind = (0, sniff_service_1.sniffXmlKind)(absIn);
    if (!kind)
        throw new Error(`Cannot detect file type (telemetry/datalink) from: ${absIn}`);
    const run = (0, factory_1.createRepackRunner)(kind);
    await run(absIn, absOut, { utcOffset, includeJso });
    console.log(`✅ Repacked [${kind}] ${absIn} → ${absOut}`);
}


/***/ }),

/***/ 210:
/***/ ((__unused_webpack_module, exports, __nccwpck_require__) => {

"use strict";

Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.createRepackRunner = createRepackRunner;
const datalink_repack_service_1 = __nccwpck_require__(211);
const telemetry_repack_service_1 = __nccwpck_require__(30);
function createRepackRunner(kind) {
    switch (kind) {
        case "telemetry":
            return telemetry_repack_service_1.repackTelemetryToApx_stream;
        case "datalink":
            return datalink_repack_service_1.repackDatalinkToApx_stream;
        default:
            throw new Error(`Unsupported kind: ${kind}`);
    }
}


/***/ }),

/***/ 650:
/***/ (function(__unused_webpack_module, exports, __nccwpck_require__) {

"use strict";

var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.ApxTlmWriter = exports.ExtId = exports.DSpec = void 0;
const fs = __importStar(__nccwpck_require__(896));
const path = __importStar(__nccwpck_require__(928));
const zlib = __importStar(__nccwpck_require__(106));
var DSpec;
(function (DSpec) {
    DSpec[DSpec["ext"] = 0] = "ext";
    DSpec[DSpec["u8"] = 1] = "u8";
    DSpec[DSpec["u16"] = 2] = "u16";
    DSpec[DSpec["u24"] = 3] = "u24";
    DSpec[DSpec["u32"] = 4] = "u32";
    DSpec[DSpec["u64"] = 5] = "u64";
    DSpec[DSpec["f16"] = 6] = "f16";
    DSpec[DSpec["f32"] = 7] = "f32";
    DSpec[DSpec["f64"] = 8] = "f64";
    DSpec[DSpec["Null"] = 9] = "Null";
    DSpec[DSpec["a16"] = 10] = "a16";
    DSpec[DSpec["a32"] = 11] = "a32";
})(DSpec || (exports.DSpec = DSpec = {}));
var ExtId;
(function (ExtId) {
    ExtId[ExtId["stop"] = 0] = "stop";
    ExtId[ExtId["ts"] = 1] = "ts";
    ExtId[ExtId["dir"] = 2] = "dir";
    ExtId[ExtId["field"] = 3] = "field";
    ExtId[ExtId["evtid"] = 4] = "evtid";
    ExtId[ExtId["evt"] = 8] = "evt";
    ExtId[ExtId["jso"] = 9] = "jso";
    ExtId[ExtId["raw"] = 10] = "raw";
    ExtId[ExtId["zip"] = 11] = "zip";
})(ExtId || (exports.ExtId = ExtId = {}));
// ----- helpers -----
function cstr(s) {
    return Buffer.concat([Buffer.from(s, "utf8"), Buffer.from([0x00])]);
}
/** APX "qCompress" payload: [len_be(4)] + deflate(data) */
function qCompress(payload) {
    const comp = zlib.deflateSync(payload);
    const out = Buffer.allocUnsafe(4 + comp.length);
    out.writeUInt32BE(payload.length >>> 0, 0);
    comp.copy(out, 4);
    return out;
}
// half-float encode/decode helpers
function f32ToF16Bits(val) {
    const f32 = new Float32Array(1);
    const u32 = new Uint32Array(f32.buffer);
    f32[0] = val;
    const x = u32[0];
    const sign = (x >>> 31) & 0x1;
    const exp = (x >>> 23) & 0xff;
    const mant = x & 0x7fffff;
    if (exp === 0xff) { // Inf/NaN
        const isNaN = mant !== 0;
        return (sign << 15) | (0x1f << 10) | (isNaN ? 0x200 : 0);
    }
    let exp16 = exp - 127 + 15;
    if (exp <= 112) { // subnormal or zero
        if (exp < 103)
            return (sign << 15);
        const shift = 113 - exp;
        let mant16 = (0x800000 | mant) >>> shift;
        if ((mant16 & 0x1) && ((mant & ((1 << (shift - 1)) - 1)) !== 0))
            mant16++;
        return (sign << 15) | mant16;
    }
    else if (exp16 >= 0x1f) {
        return (sign << 15) | (0x1f << 10);
    }
    else {
        let mant16 = mant >>> 13;
        const roundBit = (mant >>> 12) & 1;
        if (roundBit && ((mant16 & 1) || (mant & 0xFFF)))
            mant16++;
        if (mant16 === 0x400) {
            mant16 = 0;
            exp16++;
            if (exp16 >= 0x1f)
                return (sign << 15) | (0x1f << 10);
        }
        return (sign << 15) | ((exp16 & 0x1f) << 10) | (mant16 & 0x3ff);
    }
}
function f16ToF32Approx(bits) {
    const s = (bits & 0x8000) ? -1 : 1;
    const e = (bits >>> 10) & 0x1f;
    const f = bits & 0x3ff;
    if (e === 0)
        return s * Math.pow(2, -14) * (f / 1024);
    if (e === 31)
        return f ? NaN : s * Infinity;
    return s * Math.pow(2, e - 15) * (1 + f / 1024);
}
/** choose f16 when exactly reversible (within strict equality), else f32 */
function chooseFloatSpec(val) {
    if (Number.isFinite(val)) {
        const h = f32ToF16Bits(val);
        const back = f16ToF32Approx(h);
        if (Object.is(back, val)) {
            return { dspec: DSpec.f16, writer: (b, off) => b.writeUInt16LE(h, off), size: 2 };
        }
    }
    return { dspec: DSpec.f32, writer: (b, off) => b.writeFloatLE(val, off), size: 4 };
}
// ----- writer -----
class ApxTlmWriter {
    constructor(version = 1, // version 1 for repack
    utcOffsetSeconds = 0, // seconds
    startTimestampMs64 = 0, // ms epoch
    outFilePath, outStream) {
        this.version = version;
        this.utcOffsetSeconds = utcOffsetSeconds;
        this.startTimestampMs64 = startTimestampMs64;
        this.declaredFields = 0;
        this.headerWritten = false;
        this.lastWidx = -1;
        this.lastDown = new Map();
        this.lastUp = new Map();
        if (outStream) {
            this.ws = outStream;
        }
        else if (outFilePath) {
            const dir = path.dirname(outFilePath);
            fs.mkdirSync(dir, { recursive: true });
            this.ws = fs.createWriteStream(outFilePath, { highWaterMark: 100 * 1024 });
        }
        else {
            throw new Error("ApxTlmWriter: provide outFilePath or outStream");
        }
    }
    write(buf) { this.ws.write(buf); }
    pushExt(id) { this.write(Buffer.from([(id << 4) | 0x00])); }
    /** Fixed-size header placeholder. */
    writeHeaderPlaceholder() {
        if (this.headerWritten)
            return;
        const b = Buffer.alloc(44, 0);
        b.write("APXTLM", 0, "ascii");
        b.writeUInt16LE(this.version & 0xFFFF, 16);
        b.writeUInt16LE(44, 18);
        b.writeBigUInt64LE(BigInt(this.startTimestampMs64), 32);
        b.writeInt32LE(this.utcOffsetSeconds | 0, 40);
        this.write(b);
        this.headerWritten = true;
    }
    // ----- info -----
    emitInfo(info) {
        if (typeof info === "object" && info) {
            info.utc_offset = this.utcOffsetSeconds | 0;
            if (info.timestamp == null)
                info.timestamp = this.startTimestampMs64 >>> 0;
        }
        this.pushExt(ExtId.jso);
        const nameLit = this.cachedLit("info");
        const payload = Buffer.from(JSON.stringify(info), "utf8");
        const q = qCompress(payload);
        const sz = Buffer.allocUnsafe(4);
        sz.writeUInt32LE(q.length, 0);
        this.write(Buffer.concat([nameLit, sz, q]));
    }
    // ----- registry -----
    emitField(name, info = []) {
        this.pushExt(ExtId.field);
        const parts = [cstr(name), Buffer.from([info.length & 0xFF])];
        for (const s of info)
            parts.push(cstr(s));
        this.write(Buffer.concat(parts));
        this.declaredFields++;
    }
    emitEvtId(name, keys) {
        this.pushExt(ExtId.evtid);
        const parts = [cstr(name), Buffer.from([keys.length & 0xFF])];
        for (const k of keys)
            parts.push(cstr(k));
        this.write(Buffer.concat(parts));
    }
    // ----- timestamp -----
    emitTs(ms) {
        this.pushExt(ExtId.ts);
        const b = Buffer.allocUnsafe(4);
        b.writeUInt32LE(ms >>> 0, 0);
        this.write(b);
        this.lastWidx = -1;
    }
    // ----- values (optimized) -----
    writeIndexAndSpec(dspec, fieldIndex) {
        if (this.lastWidx >= 0) {
            const delta = fieldIndex - this.lastWidx - 1;
            if (delta >= 0 && delta <= 7) {
                const b0 = 0x10 | ((delta & 0x07) << 5) | (dspec & 0x0F); // opt8
                this.write(Buffer.from([b0]));
                this.lastWidx = fieldIndex;
                return;
            }
        }
        const low3 = fieldIndex & 0x07;
        const hi = (fieldIndex >> 3) & 0xFF;
        const b0 = (low3 << 5) | (dspec & 0x0F);
        const b1 = hi;
        this.write(Buffer.from([b0, b1]));
        this.lastWidx = fieldIndex;
    }
    /** write numeric value with change filtering and best packing */
    emitNumber(fieldIndex, v, uplink = false) {
        if (!(fieldIndex >= 0 && fieldIndex < this.declaredFields))
            return;
        const cache = uplink ? this.lastUp : this.lastDown;
        const prev = cache.get(fieldIndex);
        if (prev !== undefined && Object.is(prev, v))
            return;
        cache.set(fieldIndex, v);
        if (uplink)
            this.pushExt(ExtId.dir);
        const { dspec, writer, size } = chooseFloatSpec(v);
        this.writeIndexAndSpec(dspec, fieldIndex);
        const b = Buffer.allocUnsafe(size);
        writer(b, 0);
        this.write(b);
    }
    // legacy API
    emitValueF32(fieldIndex, v) { this.emitNumber(fieldIndex, v, false); }
    emitUplinkValueF32(fieldIndex, v) { this.emitNumber(fieldIndex, v, true); }
    // ----- strings/events/objects -----
    cachedLit(s) {
        return Buffer.concat([Buffer.from([0xFF]), cstr(s)]);
    }
    emitEvt(evIndex, values) {
        this.pushExt(ExtId.evt);
        const parts = [Buffer.from([evIndex & 0xFF])];
        for (const v of values)
            parts.push(this.cachedLit(v ?? ""));
        this.write(Buffer.concat(parts));
    }
    emitJso(name, obj, ts) {
        if (typeof ts === "number")
            this.emitTs(ts >>> 0);
        this.pushExt(ExtId.jso);
        const nameLit = this.cachedLit(name);
        const payload = Buffer.from(JSON.stringify(obj), "utf8");
        const q = qCompress(payload);
        const sz = Buffer.allocUnsafe(4);
        sz.writeUInt32LE(q.length, 0);
        this.write(Buffer.concat([nameLit, sz, q]));
    }
    /** choose RAW or ZIP automatically (ZIP if smaller) */
    emitRaw(name, data, ts) {
        if (typeof ts === "number")
            this.emitTs(ts >>> 0);
        const nameLit = this.cachedLit(name);
        const comp = zlib.deflateSync(data);
        const zipped = Buffer.concat([Buffer.alloc(4), comp]);
        zipped.writeUInt32BE(data.length >>> 0, 0); // qCompress header
        const useZip = zipped.length < data.length + 2;
        if (useZip) {
            this.pushExt(ExtId.zip);
            const sz = Buffer.allocUnsafe(4);
            sz.writeUInt32LE(zipped.length, 0);
            this.write(Buffer.concat([nameLit, sz, zipped]));
        }
        else {
            this.pushExt(ExtId.raw);
            if (data.length > 0xFFFF) {
                let off = 0;
                while (off < data.length) {
                    const chunk = data.subarray(off, Math.min(off + 0xFFFF, data.length));
                    const sz = Buffer.allocUnsafe(2);
                    sz.writeUInt16LE(chunk.length, 0);
                    this.write(Buffer.concat([nameLit, sz, chunk]));
                    off += chunk.length;
                }
            }
            else {
                const sz = Buffer.allocUnsafe(2);
                sz.writeUInt16LE(data.length, 0);
                this.write(Buffer.concat([nameLit, sz, data]));
            }
        }
    }
    finalizeToFile() {
        this.write(Buffer.from([0x00])); // ExtId.stop
        return new Promise((resolve, reject) => {
            this.ws.once("error", reject);
            this.ws.once("finish", resolve);
            this.ws.end();
        });
    }
    getDeclaredFieldCount() { return this.declaredFields; }
}
exports.ApxTlmWriter = ApxTlmWriter;


/***/ }),

/***/ 211:
/***/ (function(__unused_webpack_module, exports, __nccwpck_require__) {

"use strict";

var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.repackDatalinkToApx_stream = repackDatalinkToApx_stream;
const fs = __importStar(__nccwpck_require__(896));
const path = __importStar(__nccwpck_require__(928));
const sax_1 = __importDefault(__nccwpck_require__(560));
const fast_xml_parser_1 = __nccwpck_require__(591);
const apxtlm_writer_service_1 = __nccwpck_require__(650);
const info_util_1 = __nccwpck_require__(735);
const MAX_FIELDS = 2048;
const TELEMETRY_TAGS = new Set(["S", "D"]);
const EVENT_TAGS = new Set(["event", "evt"]);
const SKIP_TOP_LEVEL = new Set(["S", "D", "event", "evt", "#text", "@_"]);
const EPOCH_2000_MS = Date.UTC(2000, 0, 1);
function fileMtimeMs(p) {
    try {
        return Math.floor(fs.statSync(p).mtimeMs);
    }
    catch {
        return Date.now();
    }
}
/** normalize potential seconds→ms; reject pre-2000 values (fallback to file mtime) */
function normalizeEpochMs(n, inputFile) {
    if (!Number.isFinite(n))
        return fileMtimeMs(inputFile);
    let v = n;
    if (v < 1e12 && v >= 1e9)
        v = v * 1000; // seconds → ms
    v = Math.floor(v);
    if (v < EPOCH_2000_MS)
        return fileMtimeMs(inputFile);
    return v;
}
function declareFieldsIfNeeded(ctx, tokenCountHint) {
    if (ctx.fieldsDeclared)
        return;
    if (!ctx.writer)
        return;
    if (!ctx.fields.length) {
        const n = Math.min(tokenCountHint ?? 0, MAX_FIELDS);
        ctx.fields = Array.from({ length: n }, (_, i) => `#${i}`);
    }
    if (ctx.fields.length > MAX_FIELDS)
        ctx.fields.length = MAX_FIELDS;
    for (const f of ctx.fields)
        ctx.writer.emitField(f, []);
    ctx.fieldsDeclared = true;
}
function declareEventIfNeeded(ctx, name, keys) {
    if (!ctx.writer)
        return;
    if (!ctx.evtIndex.has(name)) {
        const idx = ctx.evtIndex.size;
        ctx.evtIndex.set(name, idx);
        ctx.writer.emitEvtId(name, keys);
    }
}
function splitTokensLoose(s) {
    return String(s).split(/[,\s;]+/).map(x => x.trim()).filter(x => x.length || x === "");
}
function openTagToString(name, attrs) {
    const a = Object.entries(attrs ?? {}).map(([k, v]) => `${k}="${String(v)}"`).join(" ");
    return a.length ? `<${name} ${a}>` : `<${name}>`;
}
function closeTagToString(name) { return `</${name}>`; }
function logToFile(ctx, message) {
    if (!ctx.logFile) {
        ctx.logFile = fs.createWriteStream(path.resolve(__dirname, "log.txt"), { flags: "a" });
    }
    ctx.logFile.write(`[${new Date().toISOString()}] ${message}\n`);
}
async function repackDatalinkToApx_stream(inputFile, outFile, opts = {}) {
    const { utcOffset = 0, includeJso = true } = opts;
    const utcOffsetSec = utcOffset | 0;
    const ctx = {
        writer: undefined,
        wroteHeader: false,
        utcOffsetSec,
        baseTs: 0,
        fields: [],
        fieldsDeclared: false,
        stack: [],
        inCsv: false,
        csvTag: "",
        csvAttrs: {},
        csvText: "",
        inEvt: false,
        evtName: "",
        evtAttrs: {},
        evtText: "",
        evtIndex: new Map(),
        capJsoActive: false,
        capJsoDepth: 0,
        capJsoName: "",
        capXml: [],
        logFile: undefined,
    };
    const parser = sax_1.default.createStream(true, { trim: false, normalize: false });
    parser.on("opentag", (tag) => {
        const name = tag.name;
        const attrs = tag.attributes;
        ctx.stack.push(name);
        if (ctx.stack.length === 1) {
            const tRaw = (attrs?.["time_ms"] ?? attrs?.["UTC"]);
            if (tRaw != null) {
                const n = Number(tRaw);
                ctx.baseTs = normalizeEpochMs(n, inputFile);
            }
            else {
                ctx.baseTs = fileMtimeMs(inputFile);
            }
            if (!ctx.writer) {
                ctx.writer = new apxtlm_writer_service_1.ApxTlmWriter(1, ctx.utcOffsetSec, ctx.baseTs, outFile);
                ctx.writer.writeHeaderPlaceholder();
                ctx.wroteHeader = true;
                // info первым блоком
                const info = (0, info_util_1.buildInfoForInput)(inputFile, "datalink", ctx.baseTs, {}, ctx.utcOffsetSec);
                ctx.writer.emitInfo(info);
            }
        }
        if (TELEMETRY_TAGS.has(name)) {
            ctx.inCsv = true;
            ctx.csvTag = name;
            ctx.csvAttrs = Object.fromEntries(Object.entries(attrs || {}).map(([k, v]) => [k, String(v)]));
            ctx.csvText = "";
            return;
        }
        if (EVENT_TAGS.has(name)) {
            ctx.inEvt = true;
            ctx.evtName = attrs?.["name"] ?? name;
            ctx.evtAttrs = Object.fromEntries(Object.entries(attrs || {}).map(([k, v]) => [k, String(v)]));
            ctx.evtText = "";
            return;
        }
        if (includeJso && ctx.stack.length === 2 && !SKIP_TOP_LEVEL.has(name)) {
            ctx.capJsoActive = true;
            ctx.capJsoDepth = ctx.stack.length;
            ctx.capJsoName = name;
            ctx.capXml = [];
            ctx.capXml.push(openTagToString(name, attrs));
            return;
        }
        if (ctx.capJsoActive)
            ctx.capXml.push(openTagToString(name, attrs));
    });
    parser.on("text", (txt) => {
        if (ctx.stack.length >= 2 && ctx.stack[ctx.stack.length - 1] === "fields" && ctx.stack.includes("mandala")) {
            const raw = (txt ?? "").trim();
            if (raw)
                ctx.fields = raw.split(/[,\s;]+/).map(s => s.trim()).filter(Boolean);
        }
        if (ctx.inCsv)
            ctx.csvText += txt;
        if (ctx.inEvt)
            ctx.evtText += txt;
        if (ctx.capJsoActive && txt)
            ctx.capXml.push(txt);
    });
    parser.on("closetag", (name) => {
        if (ctx.inCsv && name === ctx.csvTag) {
            ctx.inCsv = false;
            const tsAttr = ctx.csvAttrs["t"] ?? ctx.csvAttrs["ts"] ?? ctx.csvAttrs["time_ms"] ?? ctx.csvAttrs["UTC"];
            const ts = tsAttr != null ? Number(tsAttr) : 0;
            const parts = splitTokensLoose(ctx.csvText);
            declareFieldsIfNeeded(ctx, parts.length);
            const wr = ctx.writer;
            wr.emitTs((Number.isFinite(ts) ? ts : 0) >>> 0);
            const lim = Math.min(parts.length, ctx.fields.length || MAX_FIELDS);
            for (let i = 0; i < lim; i++) {
                const s = parts[i];
                if (s === "")
                    continue;
                const n = Number(s);
                if (Number.isFinite(n))
                    wr.emitNumber(i, n, false);
            }
        }
        if (ctx.inEvt && EVENT_TAGS.has(name)) {
            ctx.inEvt = false;
            const wr = ctx.writer;
            const a = ctx.evtAttrs;
            const evName = ctx.evtName;
            const ts = a.t != null ? Number(a.t) : 0;
            const keys = Object.keys(a).filter(k => k !== "name" && k !== "t");
            if (ctx.evtText && ctx.evtText.trim().length)
                keys.push("text");
            declareEventIfNeeded(ctx, evName, keys);
            wr.emitTs(ts >>> 0);
            const idx = ctx.evtIndex.get(evName);
            const vals = [];
            for (const k of keys)
                vals.push(k === "text" ? ctx.evtText.trim() : String(a[k] ?? ""));
            wr.emitEvt(idx, vals);
        }
        if (ctx.capJsoActive) {
            ctx.capXml.push(closeTagToString(name));
            if (ctx.stack.length === ctx.capJsoDepth && name === ctx.capJsoName) {
                try {
                    const xmlStr = ctx.capXml.join("");
                    const parser = new fast_xml_parser_1.XMLParser({
                        ignoreAttributes: false,
                        ignoreDeclaration: true,
                        attributeNamePrefix: "@_",
                        textNodeName: "#text",
                        trimValues: true,
                        allowBooleanAttributes: true,
                        parseTagValue: false
                    });
                    const obj = parser.parse(xmlStr);
                    const val = obj[ctx.capJsoName] ?? obj;
                    ctx.writer.emitJso(ctx.capJsoName, val, ctx.baseTs >>> 0);
                }
                catch (e) {
                    logToFile(ctx, `[repack][warn] JSO parse failed <${ctx.capJsoName}>: ${e?.message ?? e}`);
                }
                ctx.capJsoActive = false;
                ctx.capJsoDepth = 0;
                ctx.capJsoName = "";
                ctx.capXml = [];
            }
        }
        ctx.stack.pop();
    });
    const stream = fs.createReadStream(inputFile, { encoding: "utf8", highWaterMark: 100 * 1024 });
    await new Promise((resolve, reject) => {
        stream.on("error", reject);
        parser.on("error", reject);
        parser.on("end", resolve);
        stream.pipe(parser);
    });
    if (ctx.writer) {
        await ctx.writer.finalizeToFile();
    }
    else {
        const baseTs = fileMtimeMs(inputFile);
        const wr = new apxtlm_writer_service_1.ApxTlmWriter(1, utcOffsetSec, baseTs, outFile);
        wr.writeHeaderPlaceholder();
        wr.emitInfo((0, info_util_1.buildInfoForInput)(inputFile, "datalink", baseTs, {}, utcOffsetSec));
        await wr.finalizeToFile();
    }
    try {
        const sz = fs.statSync(outFile).size;
        logToFile(ctx, `[repack] DONE → ${path.resolve(outFile)} size=${sz} bytes`);
    }
    catch { }
}


/***/ }),

/***/ 735:
/***/ (function(__unused_webpack_module, exports, __nccwpck_require__) {

"use strict";

var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.md5File = md5File;
exports.md5FileSync = md5FileSync;
exports.buildInfoForInput = buildInfoForInput;
const fs = __importStar(__nccwpck_require__(896));
const path = __importStar(__nccwpck_require__(928));
const crypto_1 = __nccwpck_require__(982);
/** async md5 of a file */
function md5File(filePath) {
    return new Promise((resolve, reject) => {
        const h = (0, crypto_1.createHash)("md5");
        const s = fs.createReadStream(filePath);
        s.on("data", (c) => h.update(c));
        s.on("error", reject);
        s.on("end", () => resolve(h.digest("hex")));
    });
}
function md5FileSync(filePath) {
    const h = (0, crypto_1.createHash)("md5");
    const b = fs.readFileSync(filePath);
    h.update(b);
    return h.digest("hex");
}
/**
 * Build minimal 'info' block used by ground for naming and meta.
 * utcOffsetSeconds must be in seconds and matches APXTLM header.
 */
function buildInfoForInput(inputFile, kind, baseTsMs, { conf = undefined, unitName = undefined, unitType = "UAV", unitUid = undefined, } = {}, utcOffsetSeconds = 0) {
    const name = path.basename(inputFile);
    const title = path.parse(inputFile).name;
    const info = {
        conf,
        sw: undefined,
        host: undefined,
        title,
        unit: (unitName || unitUid)
            ? { name: unitName, time: baseTsMs >>> 0, type: unitType, uid: unitUid }
            : undefined,
        import: {
            name,
            title,
            format: kind,
            timestamp: baseTsMs,
        },
        timestamp: baseTsMs >>> 0,
        utc_offset: utcOffsetSeconds | 0,
    };
    return info;
}


/***/ }),

/***/ 30:
/***/ (function(__unused_webpack_module, exports, __nccwpck_require__) {

"use strict";

var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.repackTelemetryToApx_stream = repackTelemetryToApx_stream;
const fs = __importStar(__nccwpck_require__(896));
const path = __importStar(__nccwpck_require__(928));
const sax_1 = __importDefault(__nccwpck_require__(560));
const fast_xml_parser_1 = __nccwpck_require__(591);
const apxtlm_writer_service_1 = __nccwpck_require__(650);
const info_util_1 = __nccwpck_require__(735);
const MAX_FIELDS = 2048;
const EPOCH_2000_MS = Date.UTC(2000, 0, 1);
function fileMtimeMs(p) {
    try {
        return Math.floor(fs.statSync(p).mtimeMs);
    }
    catch {
        return Date.now();
    }
}
function ensureWriter(ctx) {
    if (!ctx.writer)
        throw new Error("Writer not initialized yet");
    return ctx.writer;
}
function declareFieldsIfNeeded(ctx, tokenCountHint) {
    if (ctx.fieldsDeclared)
        return;
    const wr = ensureWriter(ctx);
    if (!ctx.fields.length) {
        const n = Math.min(tokenCountHint ?? 0, MAX_FIELDS);
        ctx.fields = Array.from({ length: n }, (_, i) => `#${i}`);
    }
    if (ctx.fields.length > MAX_FIELDS)
        ctx.fields.length = MAX_FIELDS;
    for (const f of ctx.fields)
        wr.emitField(f, []);
    ctx.fieldsDeclared = true;
}
function declareEventIfNeeded(ctx, name, keys) {
    if (!ctx.writer)
        return;
    if (!ctx.evtIndex.has(name)) {
        const idx = ctx.evtIndex.size;
        ctx.evtIndex.set(name, idx);
        ctx.writer.emitEvtId(name, keys);
    }
}
function parseCsvKeepEmpty(s) {
    return String(s).split(",");
}
function openTagToString(name, attrs) {
    const a = Object.entries(attrs ?? {}).map(([k, v]) => `${k}="${String(v)}"`).join(" ");
    return a.length ? `<${name} ${a}>` : `<${name}>`;
}
function closeTagToString(name) { return `</${name}>`; }
async function repackTelemetryToApx_stream(inputFile, outFile, opts = {}) {
    const { utcOffset = 0, includeJso = true } = opts;
    const utcOffsetSec = utcOffset | 0;
    const ctx = {
        inTelemetry: false,
        inData: false,
        inFields: false,
        inD: false,
        inE: false,
        currentD_ts: 0,
        currentD_text: "",
        currentE_attrs: {},
        currentE_text: "",
        fields: [],
        fieldsDeclared: false,
        evtIndex: new Map(),
        writer: undefined,
        wroteHeader: false,
        baseTs: 0,
        utcOffsetSec,
        inDataDepth: 0,
        capJsoActive: false,
        capJsoDepth: 0,
        capJsoName: "",
        capXml: [],
    };
    const parser = sax_1.default.createStream(true, { trim: false, normalize: false });
    const jsoSkip = new Set(["D", "E", "U"]);
    // ===== open tag =====
    parser.on("opentag", (tag) => {
        const name = tag.name;
        const attrs = tag.attributes;
        if (!ctx.inTelemetry && name.toLowerCase() === "telemetry") {
            ctx.inTelemetry = true;
            return;
        }
        if (!ctx.inTelemetry)
            return;
        if (name === "fields") {
            ctx.inFields = true;
            return;
        }
        if (name === "info") {
            if (attrs && attrs["time"] != null) {
                const t = Number(attrs["time"]);
                if (Number.isFinite(t))
                    ctx.baseTs = Math.floor(t);
            }
        }
        if (name === "timestamp" && attrs && attrs["value"]) {
            const t = Date.parse(String(attrs["value"]));
            if (Number.isFinite(t))
                ctx.baseTs = Math.floor(t);
        }
        if (name === "data") {
            ctx.inData = true;
            ctx.inDataDepth = 1;
            // нормализуем baseTs или берём mtime файла
            if (!(ctx.baseTs >= EPOCH_2000_MS))
                ctx.baseTs = fileMtimeMs(inputFile);
            if (!ctx.wroteHeader) {
                ctx.writer = new apxtlm_writer_service_1.ApxTlmWriter(1, ctx.utcOffsetSec, ctx.baseTs, outFile);
                ctx.writer.writeHeaderPlaceholder();
                // info самым первым
                ctx.writer.emitInfo((0, info_util_1.buildInfoForInput)(inputFile, "telemetry", ctx.baseTs, {}, ctx.utcOffsetSec));
                ctx.wroteHeader = true;
            }
            return;
        }
        if (!ctx.inData)
            return;
        if (name === "D") {
            ctx.inD = true;
            ctx.currentD_ts = attrs?.["t"] != null ? Number(attrs["t"]) >>> 0 : 0;
            ctx.currentD_text = "";
            return;
        }
        if (name === "E") {
            ctx.inE = true;
            ctx.currentE_attrs = Object.fromEntries(Object.entries(attrs || {}).map(([k, v]) => [k, String(v)]));
            ctx.currentE_text = "";
            return;
        }
        if (includeJso && !jsoSkip.has(name)) {
            if (!ctx.capJsoActive) {
                ctx.capJsoActive = true;
                ctx.capJsoDepth = ctx.inDataDepth + 1;
                ctx.capJsoName = name;
                ctx.capXml = [];
            }
            ctx.capXml.push(openTagToString(name, attrs));
        }
        if (ctx.inData)
            ctx.inDataDepth++;
    });
    // ===== text =====
    parser.on("text", (txt) => {
        if (ctx.inFields) {
            const raw = (txt ?? "").trim();
            if (raw)
                ctx.fields = raw.split(",").map(s => s.trim()).filter(Boolean);
        }
        if (ctx.inD)
            ctx.currentD_text += txt;
        if (ctx.inE)
            ctx.currentE_text += txt;
        if (ctx.capJsoActive && txt)
            ctx.capXml.push(txt);
    });
    // ===== close tag =====
    parser.on("closetag", (name) => {
        if (name === "fields") {
            ctx.inFields = false;
            return;
        }
        if (name === "D" && ctx.inD) {
            ctx.inD = false;
            declareFieldsIfNeeded(ctx, parseCsvKeepEmpty(ctx.currentD_text).length);
            const wr = ctx.writer;
            wr.emitTs(ctx.currentD_ts >>> 0);
            const parts = parseCsvKeepEmpty(ctx.currentD_text);
            const lim = Math.min(parts.length, ctx.fields.length || MAX_FIELDS);
            for (let i = 0; i < lim; i++) {
                const s = parts[i];
                if (s === "")
                    continue;
                const n = Number(s);
                if (Number.isFinite(n))
                    wr.emitNumber(i, n, false);
            }
            return;
        }
        if (name === "E" && ctx.inE) {
            ctx.inE = false;
            const wr = ctx.writer;
            const a = ctx.currentE_attrs;
            const evName = a.name ?? "event";
            const ts = a.t != null ? Number(a.t) : 0;
            const keys = Object.keys(a).filter(k => k !== "name" && k !== "t");
            if (ctx.currentE_text && ctx.currentE_text.trim().length)
                keys.push("text");
            declareEventIfNeeded(ctx, evName, keys);
            wr.emitTs(ts >>> 0);
            const idx = ctx.evtIndex.get(evName);
            const vals = [];
            for (const k of keys)
                vals.push(k === "text" ? ctx.currentE_text.trim() : String(a[k] ?? ""));
            wr.emitEvt(idx, vals);
            return;
        }
        if (ctx.capJsoActive) {
            ctx.capXml.push(closeTagToString(name));
            if (name === ctx.capJsoName && ctx.inDataDepth === ctx.capJsoDepth) {
                try {
                    const xmlStr = ctx.capXml.join("");
                    const parser = new fast_xml_parser_1.XMLParser({
                        ignoreAttributes: false,
                        ignoreDeclaration: true,
                        attributeNamePrefix: "@_",
                        textNodeName: "#text",
                        trimValues: true,
                        allowBooleanAttributes: true,
                        parseTagValue: false
                    });
                    const obj = parser.parse(xmlStr);
                    const val = obj[ctx.capJsoName] ?? obj;
                    ctx.writer.emitJso(ctx.capJsoName, val, ctx.baseTs >>> 0);
                }
                catch (e) {
                    console.warn(`[repack][warn] JSO parse failed <${ctx.capJsoName}>: ${e?.message ?? e}`);
                }
                ctx.capJsoActive = false;
                ctx.capJsoDepth = 0;
                ctx.capJsoName = "";
                ctx.capXml = [];
            }
        }
        if (ctx.inData)
            ctx.inDataDepth--;
        if (name.toLowerCase() === "telemetry")
            ctx.inTelemetry = false;
        if (name === "data")
            ctx.inData = false;
    });
    // ===== errors / finish =====
    const stream = fs.createReadStream(inputFile, { encoding: "utf8", highWaterMark: 100 * 1024 });
    await new Promise((resolve, reject) => {
        stream.on("error", reject);
        parser.on("error", reject);
        parser.on("end", resolve);
        stream.pipe(parser);
    });
    if (ctx.writer) {
        await ctx.writer.finalizeToFile();
    }
    else {
        const baseTs = fileMtimeMs(inputFile);
        const wr = new apxtlm_writer_service_1.ApxTlmWriter(1, utcOffsetSec, baseTs, outFile);
        wr.writeHeaderPlaceholder();
        wr.emitInfo((0, info_util_1.buildInfoForInput)(inputFile, "telemetry", baseTs, {}, utcOffsetSec));
        await wr.finalizeToFile();
    }
    try {
        const sz = fs.statSync(outFile).size;
        console.log(`[repack] DONE → ${path.resolve(outFile)} size=${sz} bytes`);
    }
    catch { }
}


/***/ }),

/***/ 521:
/***/ (function(__unused_webpack_module, exports, __nccwpck_require__) {

"use strict";

var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.sniffXmlKind = sniffXmlKind;
const fs = __importStar(__nccwpck_require__(896));
const path = __importStar(__nccwpck_require__(928));
const fast_xml_parser_1 = __nccwpck_require__(591);
/** Cheap, defensive sniff of file kind by extension, content head and XML tags. */
function sniffXmlKind(filePath) {
    const fd = fs.openSync(filePath, "r");
    try {
        const buf = Buffer.allocUnsafe(64 * 1024);
        const n = fs.readSync(fd, buf, 0, buf.length, 0);
        const head = buf.slice(0, Math.max(0, n)).toString("utf8");
        const lower = head.toLowerCase();
        // Extension hints
        const base = path.basename(filePath).toLowerCase();
        if (base.endsWith(".telemetry"))
            return "telemetry";
        if (base.endsWith(".datalink.xml") || base.includes(".datalink"))
            return "datalink";
        // Text heuristics
        if (lower.includes("<telemetry"))
            return "telemetry";
        if (lower.includes("<mandala") || lower.includes("<s>") || lower.includes("<d>"))
            return "datalink";
        // Fallback: try parsing the head
        try {
            const parser = new fast_xml_parser_1.XMLParser({
                ignoreAttributes: false,
                ignoreDeclaration: true,
                attributeNamePrefix: "@_",
                textNodeName: "#text",
                trimValues: true,
                allowBooleanAttributes: true,
                parseTagValue: false
            });
            const doc = parser.parse(head);
            const keys = Object.keys(doc).filter(k => !k.startsWith("?"));
            if (keys.some(k => k.toLowerCase().includes("telemetry")))
                return "telemetry";
            if (keys.some(k => k.toLowerCase().includes("datalink") || k.toLowerCase().includes("mandala")))
                return "datalink";
        }
        catch { /* ignore */ }
        return null;
    }
    finally {
        fs.closeSync(fd);
    }
}


/***/ }),

/***/ 982:
/***/ ((module) => {

"use strict";
module.exports = require("crypto");

/***/ }),

/***/ 896:
/***/ ((module) => {

"use strict";
module.exports = require("fs");

/***/ }),

/***/ 928:
/***/ ((module) => {

"use strict";
module.exports = require("path");

/***/ }),

/***/ 203:
/***/ ((module) => {

"use strict";
module.exports = require("stream");

/***/ }),

/***/ 193:
/***/ ((module) => {

"use strict";
module.exports = require("string_decoder");

/***/ }),

/***/ 106:
/***/ ((module) => {

"use strict";
module.exports = require("zlib");

/***/ }),

/***/ 591:
/***/ ((module) => {

(()=>{"use strict";var t={d:(e,n)=>{for(var i in n)t.o(n,i)&&!t.o(e,i)&&Object.defineProperty(e,i,{enumerable:!0,get:n[i]})},o:(t,e)=>Object.prototype.hasOwnProperty.call(t,e),r:t=>{"undefined"!=typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(t,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(t,"__esModule",{value:!0})}},e={};t.r(e),t.d(e,{XMLBuilder:()=>ft,XMLParser:()=>st,XMLValidator:()=>mt});const n=":A-Za-z_\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u02FF\\u0370-\\u037D\\u037F-\\u1FFF\\u200C-\\u200D\\u2070-\\u218F\\u2C00-\\u2FEF\\u3001-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFFD",i=new RegExp("^["+n+"]["+n+"\\-.\\d\\u00B7\\u0300-\\u036F\\u203F-\\u2040]*$");function s(t,e){const n=[];let i=e.exec(t);for(;i;){const s=[];s.startIndex=e.lastIndex-i[0].length;const r=i.length;for(let t=0;t<r;t++)s.push(i[t]);n.push(s),i=e.exec(t)}return n}const r=function(t){return!(null==i.exec(t))},o={allowBooleanAttributes:!1,unpairedTags:[]};function a(t,e){e=Object.assign({},o,e);const n=[];let i=!1,s=!1;"\ufeff"===t[0]&&(t=t.substr(1));for(let o=0;o<t.length;o++)if("<"===t[o]&&"?"===t[o+1]){if(o+=2,o=u(t,o),o.err)return o}else{if("<"!==t[o]){if(l(t[o]))continue;return x("InvalidChar","char '"+t[o]+"' is not expected.",N(t,o))}{let a=o;if(o++,"!"===t[o]){o=h(t,o);continue}{let d=!1;"/"===t[o]&&(d=!0,o++);let f="";for(;o<t.length&&">"!==t[o]&&" "!==t[o]&&"\t"!==t[o]&&"\n"!==t[o]&&"\r"!==t[o];o++)f+=t[o];if(f=f.trim(),"/"===f[f.length-1]&&(f=f.substring(0,f.length-1),o--),!r(f)){let e;return e=0===f.trim().length?"Invalid space after '<'.":"Tag '"+f+"' is an invalid name.",x("InvalidTag",e,N(t,o))}const p=c(t,o);if(!1===p)return x("InvalidAttr","Attributes for '"+f+"' have open quote.",N(t,o));let b=p.value;if(o=p.index,"/"===b[b.length-1]){const n=o-b.length;b=b.substring(0,b.length-1);const s=g(b,e);if(!0!==s)return x(s.err.code,s.err.msg,N(t,n+s.err.line));i=!0}else if(d){if(!p.tagClosed)return x("InvalidTag","Closing tag '"+f+"' doesn't have proper closing.",N(t,o));if(b.trim().length>0)return x("InvalidTag","Closing tag '"+f+"' can't have attributes or invalid starting.",N(t,a));if(0===n.length)return x("InvalidTag","Closing tag '"+f+"' has not been opened.",N(t,a));{const e=n.pop();if(f!==e.tagName){let n=N(t,e.tagStartPos);return x("InvalidTag","Expected closing tag '"+e.tagName+"' (opened in line "+n.line+", col "+n.col+") instead of closing tag '"+f+"'.",N(t,a))}0==n.length&&(s=!0)}}else{const r=g(b,e);if(!0!==r)return x(r.err.code,r.err.msg,N(t,o-b.length+r.err.line));if(!0===s)return x("InvalidXml","Multiple possible root nodes found.",N(t,o));-1!==e.unpairedTags.indexOf(f)||n.push({tagName:f,tagStartPos:a}),i=!0}for(o++;o<t.length;o++)if("<"===t[o]){if("!"===t[o+1]){o++,o=h(t,o);continue}if("?"!==t[o+1])break;if(o=u(t,++o),o.err)return o}else if("&"===t[o]){const e=m(t,o);if(-1==e)return x("InvalidChar","char '&' is not expected.",N(t,o));o=e}else if(!0===s&&!l(t[o]))return x("InvalidXml","Extra text at the end",N(t,o));"<"===t[o]&&o--}}}return i?1==n.length?x("InvalidTag","Unclosed tag '"+n[0].tagName+"'.",N(t,n[0].tagStartPos)):!(n.length>0)||x("InvalidXml","Invalid '"+JSON.stringify(n.map((t=>t.tagName)),null,4).replace(/\r?\n/g,"")+"' found.",{line:1,col:1}):x("InvalidXml","Start tag expected.",1)}function l(t){return" "===t||"\t"===t||"\n"===t||"\r"===t}function u(t,e){const n=e;for(;e<t.length;e++)if("?"!=t[e]&&" "!=t[e]);else{const i=t.substr(n,e-n);if(e>5&&"xml"===i)return x("InvalidXml","XML declaration allowed only at the start of the document.",N(t,e));if("?"==t[e]&&">"==t[e+1]){e++;break}}return e}function h(t,e){if(t.length>e+5&&"-"===t[e+1]&&"-"===t[e+2]){for(e+=3;e<t.length;e++)if("-"===t[e]&&"-"===t[e+1]&&">"===t[e+2]){e+=2;break}}else if(t.length>e+8&&"D"===t[e+1]&&"O"===t[e+2]&&"C"===t[e+3]&&"T"===t[e+4]&&"Y"===t[e+5]&&"P"===t[e+6]&&"E"===t[e+7]){let n=1;for(e+=8;e<t.length;e++)if("<"===t[e])n++;else if(">"===t[e]&&(n--,0===n))break}else if(t.length>e+9&&"["===t[e+1]&&"C"===t[e+2]&&"D"===t[e+3]&&"A"===t[e+4]&&"T"===t[e+5]&&"A"===t[e+6]&&"["===t[e+7])for(e+=8;e<t.length;e++)if("]"===t[e]&&"]"===t[e+1]&&">"===t[e+2]){e+=2;break}return e}const d='"',f="'";function c(t,e){let n="",i="",s=!1;for(;e<t.length;e++){if(t[e]===d||t[e]===f)""===i?i=t[e]:i!==t[e]||(i="");else if(">"===t[e]&&""===i){s=!0;break}n+=t[e]}return""===i&&{value:n,index:e,tagClosed:s}}const p=new RegExp("(\\s*)([^\\s=]+)(\\s*=)?(\\s*(['\"])(([\\s\\S])*?)\\5)?","g");function g(t,e){const n=s(t,p),i={};for(let t=0;t<n.length;t++){if(0===n[t][1].length)return x("InvalidAttr","Attribute '"+n[t][2]+"' has no space in starting.",E(n[t]));if(void 0!==n[t][3]&&void 0===n[t][4])return x("InvalidAttr","Attribute '"+n[t][2]+"' is without value.",E(n[t]));if(void 0===n[t][3]&&!e.allowBooleanAttributes)return x("InvalidAttr","boolean attribute '"+n[t][2]+"' is not allowed.",E(n[t]));const s=n[t][2];if(!b(s))return x("InvalidAttr","Attribute '"+s+"' is an invalid name.",E(n[t]));if(i.hasOwnProperty(s))return x("InvalidAttr","Attribute '"+s+"' is repeated.",E(n[t]));i[s]=1}return!0}function m(t,e){if(";"===t[++e])return-1;if("#"===t[e])return function(t,e){let n=/\d/;for("x"===t[e]&&(e++,n=/[\da-fA-F]/);e<t.length;e++){if(";"===t[e])return e;if(!t[e].match(n))break}return-1}(t,++e);let n=0;for(;e<t.length;e++,n++)if(!(t[e].match(/\w/)&&n<20)){if(";"===t[e])break;return-1}return e}function x(t,e,n){return{err:{code:t,msg:e,line:n.line||n,col:n.col}}}function b(t){return r(t)}function N(t,e){const n=t.substring(0,e).split(/\r?\n/);return{line:n.length,col:n[n.length-1].length+1}}function E(t){return t.startIndex+t[1].length}const v={preserveOrder:!1,attributeNamePrefix:"@_",attributesGroupName:!1,textNodeName:"#text",ignoreAttributes:!0,removeNSPrefix:!1,allowBooleanAttributes:!1,parseTagValue:!0,parseAttributeValue:!1,trimValues:!0,cdataPropName:!1,numberParseOptions:{hex:!0,leadingZeros:!0,eNotation:!0},tagValueProcessor:function(t,e){return e},attributeValueProcessor:function(t,e){return e},stopNodes:[],alwaysCreateTextNode:!1,isArray:()=>!1,commentPropName:!1,unpairedTags:[],processEntities:!0,htmlEntities:!1,ignoreDeclaration:!1,ignorePiTags:!1,transformTagName:!1,transformAttributeName:!1,updateTag:function(t,e,n){return t},captureMetaData:!1};let y;y="function"!=typeof Symbol?"@@xmlMetadata":Symbol("XML Node Metadata");class T{constructor(t){this.tagname=t,this.child=[],this[":@"]={}}add(t,e){"__proto__"===t&&(t="#__proto__"),this.child.push({[t]:e})}addChild(t,e){"__proto__"===t.tagname&&(t.tagname="#__proto__"),t[":@"]&&Object.keys(t[":@"]).length>0?this.child.push({[t.tagname]:t.child,":@":t[":@"]}):this.child.push({[t.tagname]:t.child}),void 0!==e&&(this.child[this.child.length-1][y]={startIndex:e})}static getMetaDataSymbol(){return y}}function w(t,e){const n={};if("O"!==t[e+3]||"C"!==t[e+4]||"T"!==t[e+5]||"Y"!==t[e+6]||"P"!==t[e+7]||"E"!==t[e+8])throw new Error("Invalid Tag instead of DOCTYPE");{e+=9;let i=1,s=!1,r=!1,o="";for(;e<t.length;e++)if("<"!==t[e]||r)if(">"===t[e]){if(r?"-"===t[e-1]&&"-"===t[e-2]&&(r=!1,i--):i--,0===i)break}else"["===t[e]?s=!0:o+=t[e];else{if(s&&C(t,"!ENTITY",e)){let i,s;e+=7,[i,s,e]=O(t,e+1),-1===s.indexOf("&")&&(n[i]={regx:RegExp(`&${i};`,"g"),val:s})}else if(s&&C(t,"!ELEMENT",e)){e+=8;const{index:n}=S(t,e+1);e=n}else if(s&&C(t,"!ATTLIST",e))e+=8;else if(s&&C(t,"!NOTATION",e)){e+=9;const{index:n}=A(t,e+1);e=n}else{if(!C(t,"!--",e))throw new Error("Invalid DOCTYPE");r=!0}i++,o=""}if(0!==i)throw new Error("Unclosed DOCTYPE")}return{entities:n,i:e}}const P=(t,e)=>{for(;e<t.length&&/\s/.test(t[e]);)e++;return e};function O(t,e){e=P(t,e);let n="";for(;e<t.length&&!/\s/.test(t[e])&&'"'!==t[e]&&"'"!==t[e];)n+=t[e],e++;if($(n),e=P(t,e),"SYSTEM"===t.substring(e,e+6).toUpperCase())throw new Error("External entities are not supported");if("%"===t[e])throw new Error("Parameter entities are not supported");let i="";return[e,i]=I(t,e,"entity"),[n,i,--e]}function A(t,e){e=P(t,e);let n="";for(;e<t.length&&!/\s/.test(t[e]);)n+=t[e],e++;$(n),e=P(t,e);const i=t.substring(e,e+6).toUpperCase();if("SYSTEM"!==i&&"PUBLIC"!==i)throw new Error(`Expected SYSTEM or PUBLIC, found "${i}"`);e+=i.length,e=P(t,e);let s=null,r=null;if("PUBLIC"===i)[e,s]=I(t,e,"publicIdentifier"),'"'!==t[e=P(t,e)]&&"'"!==t[e]||([e,r]=I(t,e,"systemIdentifier"));else if("SYSTEM"===i&&([e,r]=I(t,e,"systemIdentifier"),!r))throw new Error("Missing mandatory system identifier for SYSTEM notation");return{notationName:n,publicIdentifier:s,systemIdentifier:r,index:--e}}function I(t,e,n){let i="";const s=t[e];if('"'!==s&&"'"!==s)throw new Error(`Expected quoted string, found "${s}"`);for(e++;e<t.length&&t[e]!==s;)i+=t[e],e++;if(t[e]!==s)throw new Error(`Unterminated ${n} value`);return[++e,i]}function S(t,e){e=P(t,e);let n="";for(;e<t.length&&!/\s/.test(t[e]);)n+=t[e],e++;if(!$(n))throw new Error(`Invalid element name: "${n}"`);let i="";if("E"===t[e=P(t,e)]&&C(t,"MPTY",e))e+=4;else if("A"===t[e]&&C(t,"NY",e))e+=2;else{if("("!==t[e])throw new Error(`Invalid Element Expression, found "${t[e]}"`);for(e++;e<t.length&&")"!==t[e];)i+=t[e],e++;if(")"!==t[e])throw new Error("Unterminated content model")}return{elementName:n,contentModel:i.trim(),index:e}}function C(t,e,n){for(let i=0;i<e.length;i++)if(e[i]!==t[n+i+1])return!1;return!0}function $(t){if(r(t))return t;throw new Error(`Invalid entity name ${t}`)}const j=/^[-+]?0x[a-fA-F0-9]+$/,D=/^([\-\+])?(0*)([0-9]*(\.[0-9]*)?)$/,V={hex:!0,leadingZeros:!0,decimalPoint:".",eNotation:!0};const M=/^([-+])?(0*)(\d*(\.\d*)?[eE][-\+]?\d+)$/;function _(t){return"function"==typeof t?t:Array.isArray(t)?e=>{for(const n of t){if("string"==typeof n&&e===n)return!0;if(n instanceof RegExp&&n.test(e))return!0}}:()=>!1}class k{constructor(t){this.options=t,this.currentNode=null,this.tagsNodeStack=[],this.docTypeEntities={},this.lastEntities={apos:{regex:/&(apos|#39|#x27);/g,val:"'"},gt:{regex:/&(gt|#62|#x3E);/g,val:">"},lt:{regex:/&(lt|#60|#x3C);/g,val:"<"},quot:{regex:/&(quot|#34|#x22);/g,val:'"'}},this.ampEntity={regex:/&(amp|#38|#x26);/g,val:"&"},this.htmlEntities={space:{regex:/&(nbsp|#160);/g,val:" "},cent:{regex:/&(cent|#162);/g,val:"¢"},pound:{regex:/&(pound|#163);/g,val:"£"},yen:{regex:/&(yen|#165);/g,val:"¥"},euro:{regex:/&(euro|#8364);/g,val:"€"},copyright:{regex:/&(copy|#169);/g,val:"©"},reg:{regex:/&(reg|#174);/g,val:"®"},inr:{regex:/&(inr|#8377);/g,val:"₹"},num_dec:{regex:/&#([0-9]{1,7});/g,val:(t,e)=>String.fromCodePoint(Number.parseInt(e,10))},num_hex:{regex:/&#x([0-9a-fA-F]{1,6});/g,val:(t,e)=>String.fromCodePoint(Number.parseInt(e,16))}},this.addExternalEntities=F,this.parseXml=X,this.parseTextData=L,this.resolveNameSpace=B,this.buildAttributesMap=G,this.isItStopNode=Z,this.replaceEntitiesValue=R,this.readStopNodeData=J,this.saveTextToParentTag=q,this.addChild=Y,this.ignoreAttributesFn=_(this.options.ignoreAttributes)}}function F(t){const e=Object.keys(t);for(let n=0;n<e.length;n++){const i=e[n];this.lastEntities[i]={regex:new RegExp("&"+i+";","g"),val:t[i]}}}function L(t,e,n,i,s,r,o){if(void 0!==t&&(this.options.trimValues&&!i&&(t=t.trim()),t.length>0)){o||(t=this.replaceEntitiesValue(t));const i=this.options.tagValueProcessor(e,t,n,s,r);return null==i?t:typeof i!=typeof t||i!==t?i:this.options.trimValues||t.trim()===t?H(t,this.options.parseTagValue,this.options.numberParseOptions):t}}function B(t){if(this.options.removeNSPrefix){const e=t.split(":"),n="/"===t.charAt(0)?"/":"";if("xmlns"===e[0])return"";2===e.length&&(t=n+e[1])}return t}const U=new RegExp("([^\\s=]+)\\s*(=\\s*(['\"])([\\s\\S]*?)\\3)?","gm");function G(t,e,n){if(!0!==this.options.ignoreAttributes&&"string"==typeof t){const n=s(t,U),i=n.length,r={};for(let t=0;t<i;t++){const i=this.resolveNameSpace(n[t][1]);if(this.ignoreAttributesFn(i,e))continue;let s=n[t][4],o=this.options.attributeNamePrefix+i;if(i.length)if(this.options.transformAttributeName&&(o=this.options.transformAttributeName(o)),"__proto__"===o&&(o="#__proto__"),void 0!==s){this.options.trimValues&&(s=s.trim()),s=this.replaceEntitiesValue(s);const t=this.options.attributeValueProcessor(i,s,e);r[o]=null==t?s:typeof t!=typeof s||t!==s?t:H(s,this.options.parseAttributeValue,this.options.numberParseOptions)}else this.options.allowBooleanAttributes&&(r[o]=!0)}if(!Object.keys(r).length)return;if(this.options.attributesGroupName){const t={};return t[this.options.attributesGroupName]=r,t}return r}}const X=function(t){t=t.replace(/\r\n?/g,"\n");const e=new T("!xml");let n=e,i="",s="";for(let r=0;r<t.length;r++)if("<"===t[r])if("/"===t[r+1]){const e=W(t,">",r,"Closing Tag is not closed.");let o=t.substring(r+2,e).trim();if(this.options.removeNSPrefix){const t=o.indexOf(":");-1!==t&&(o=o.substr(t+1))}this.options.transformTagName&&(o=this.options.transformTagName(o)),n&&(i=this.saveTextToParentTag(i,n,s));const a=s.substring(s.lastIndexOf(".")+1);if(o&&-1!==this.options.unpairedTags.indexOf(o))throw new Error(`Unpaired tag can not be used as closing tag: </${o}>`);let l=0;a&&-1!==this.options.unpairedTags.indexOf(a)?(l=s.lastIndexOf(".",s.lastIndexOf(".")-1),this.tagsNodeStack.pop()):l=s.lastIndexOf("."),s=s.substring(0,l),n=this.tagsNodeStack.pop(),i="",r=e}else if("?"===t[r+1]){let e=z(t,r,!1,"?>");if(!e)throw new Error("Pi Tag is not closed.");if(i=this.saveTextToParentTag(i,n,s),this.options.ignoreDeclaration&&"?xml"===e.tagName||this.options.ignorePiTags);else{const t=new T(e.tagName);t.add(this.options.textNodeName,""),e.tagName!==e.tagExp&&e.attrExpPresent&&(t[":@"]=this.buildAttributesMap(e.tagExp,s,e.tagName)),this.addChild(n,t,s,r)}r=e.closeIndex+1}else if("!--"===t.substr(r+1,3)){const e=W(t,"--\x3e",r+4,"Comment is not closed.");if(this.options.commentPropName){const o=t.substring(r+4,e-2);i=this.saveTextToParentTag(i,n,s),n.add(this.options.commentPropName,[{[this.options.textNodeName]:o}])}r=e}else if("!D"===t.substr(r+1,2)){const e=w(t,r);this.docTypeEntities=e.entities,r=e.i}else if("!["===t.substr(r+1,2)){const e=W(t,"]]>",r,"CDATA is not closed.")-2,o=t.substring(r+9,e);i=this.saveTextToParentTag(i,n,s);let a=this.parseTextData(o,n.tagname,s,!0,!1,!0,!0);null==a&&(a=""),this.options.cdataPropName?n.add(this.options.cdataPropName,[{[this.options.textNodeName]:o}]):n.add(this.options.textNodeName,a),r=e+2}else{let o=z(t,r,this.options.removeNSPrefix),a=o.tagName;const l=o.rawTagName;let u=o.tagExp,h=o.attrExpPresent,d=o.closeIndex;this.options.transformTagName&&(a=this.options.transformTagName(a)),n&&i&&"!xml"!==n.tagname&&(i=this.saveTextToParentTag(i,n,s,!1));const f=n;f&&-1!==this.options.unpairedTags.indexOf(f.tagname)&&(n=this.tagsNodeStack.pop(),s=s.substring(0,s.lastIndexOf("."))),a!==e.tagname&&(s+=s?"."+a:a);const c=r;if(this.isItStopNode(this.options.stopNodes,s,a)){let e="";if(u.length>0&&u.lastIndexOf("/")===u.length-1)"/"===a[a.length-1]?(a=a.substr(0,a.length-1),s=s.substr(0,s.length-1),u=a):u=u.substr(0,u.length-1),r=o.closeIndex;else if(-1!==this.options.unpairedTags.indexOf(a))r=o.closeIndex;else{const n=this.readStopNodeData(t,l,d+1);if(!n)throw new Error(`Unexpected end of ${l}`);r=n.i,e=n.tagContent}const i=new T(a);a!==u&&h&&(i[":@"]=this.buildAttributesMap(u,s,a)),e&&(e=this.parseTextData(e,a,s,!0,h,!0,!0)),s=s.substr(0,s.lastIndexOf(".")),i.add(this.options.textNodeName,e),this.addChild(n,i,s,c)}else{if(u.length>0&&u.lastIndexOf("/")===u.length-1){"/"===a[a.length-1]?(a=a.substr(0,a.length-1),s=s.substr(0,s.length-1),u=a):u=u.substr(0,u.length-1),this.options.transformTagName&&(a=this.options.transformTagName(a));const t=new T(a);a!==u&&h&&(t[":@"]=this.buildAttributesMap(u,s,a)),this.addChild(n,t,s,c),s=s.substr(0,s.lastIndexOf("."))}else{const t=new T(a);this.tagsNodeStack.push(n),a!==u&&h&&(t[":@"]=this.buildAttributesMap(u,s,a)),this.addChild(n,t,s,c),n=t}i="",r=d}}else i+=t[r];return e.child};function Y(t,e,n,i){this.options.captureMetaData||(i=void 0);const s=this.options.updateTag(e.tagname,n,e[":@"]);!1===s||("string"==typeof s?(e.tagname=s,t.addChild(e,i)):t.addChild(e,i))}const R=function(t){if(this.options.processEntities){for(let e in this.docTypeEntities){const n=this.docTypeEntities[e];t=t.replace(n.regx,n.val)}for(let e in this.lastEntities){const n=this.lastEntities[e];t=t.replace(n.regex,n.val)}if(this.options.htmlEntities)for(let e in this.htmlEntities){const n=this.htmlEntities[e];t=t.replace(n.regex,n.val)}t=t.replace(this.ampEntity.regex,this.ampEntity.val)}return t};function q(t,e,n,i){return t&&(void 0===i&&(i=0===e.child.length),void 0!==(t=this.parseTextData(t,e.tagname,n,!1,!!e[":@"]&&0!==Object.keys(e[":@"]).length,i))&&""!==t&&e.add(this.options.textNodeName,t),t=""),t}function Z(t,e,n){const i="*."+n;for(const n in t){const s=t[n];if(i===s||e===s)return!0}return!1}function W(t,e,n,i){const s=t.indexOf(e,n);if(-1===s)throw new Error(i);return s+e.length-1}function z(t,e,n,i=">"){const s=function(t,e,n=">"){let i,s="";for(let r=e;r<t.length;r++){let e=t[r];if(i)e===i&&(i="");else if('"'===e||"'"===e)i=e;else if(e===n[0]){if(!n[1])return{data:s,index:r};if(t[r+1]===n[1])return{data:s,index:r}}else"\t"===e&&(e=" ");s+=e}}(t,e+1,i);if(!s)return;let r=s.data;const o=s.index,a=r.search(/\s/);let l=r,u=!0;-1!==a&&(l=r.substring(0,a),r=r.substring(a+1).trimStart());const h=l;if(n){const t=l.indexOf(":");-1!==t&&(l=l.substr(t+1),u=l!==s.data.substr(t+1))}return{tagName:l,tagExp:r,closeIndex:o,attrExpPresent:u,rawTagName:h}}function J(t,e,n){const i=n;let s=1;for(;n<t.length;n++)if("<"===t[n])if("/"===t[n+1]){const r=W(t,">",n,`${e} is not closed`);if(t.substring(n+2,r).trim()===e&&(s--,0===s))return{tagContent:t.substring(i,n),i:r};n=r}else if("?"===t[n+1])n=W(t,"?>",n+1,"StopNode is not closed.");else if("!--"===t.substr(n+1,3))n=W(t,"--\x3e",n+3,"StopNode is not closed.");else if("!["===t.substr(n+1,2))n=W(t,"]]>",n,"StopNode is not closed.")-2;else{const i=z(t,n,">");i&&((i&&i.tagName)===e&&"/"!==i.tagExp[i.tagExp.length-1]&&s++,n=i.closeIndex)}}function H(t,e,n){if(e&&"string"==typeof t){const e=t.trim();return"true"===e||"false"!==e&&function(t,e={}){if(e=Object.assign({},V,e),!t||"string"!=typeof t)return t;let n=t.trim();if(void 0!==e.skipLike&&e.skipLike.test(n))return t;if("0"===t)return 0;if(e.hex&&j.test(n))return function(t){if(parseInt)return parseInt(t,16);if(Number.parseInt)return Number.parseInt(t,16);if(window&&window.parseInt)return window.parseInt(t,16);throw new Error("parseInt, Number.parseInt, window.parseInt are not supported")}(n);if(-1!==n.search(/.+[eE].+/))return function(t,e,n){if(!n.eNotation)return t;const i=e.match(M);if(i){let s=i[1]||"";const r=-1===i[3].indexOf("e")?"E":"e",o=i[2],a=s?t[o.length+1]===r:t[o.length]===r;return o.length>1&&a?t:1!==o.length||!i[3].startsWith(`.${r}`)&&i[3][0]!==r?n.leadingZeros&&!a?(e=(i[1]||"")+i[3],Number(e)):t:Number(e)}return t}(t,n,e);{const s=D.exec(n);if(s){const r=s[1]||"",o=s[2];let a=(i=s[3])&&-1!==i.indexOf(".")?("."===(i=i.replace(/0+$/,""))?i="0":"."===i[0]?i="0"+i:"."===i[i.length-1]&&(i=i.substring(0,i.length-1)),i):i;const l=r?"."===t[o.length+1]:"."===t[o.length];if(!e.leadingZeros&&(o.length>1||1===o.length&&!l))return t;{const i=Number(n),s=String(i);if(0===i||-0===i)return i;if(-1!==s.search(/[eE]/))return e.eNotation?i:t;if(-1!==n.indexOf("."))return"0"===s||s===a||s===`${r}${a}`?i:t;let l=o?a:n;return o?l===s||r+l===s?i:t:l===s||l===r+s?i:t}}return t}var i}(t,n)}return void 0!==t?t:""}const K=T.getMetaDataSymbol();function Q(t,e){return tt(t,e)}function tt(t,e,n){let i;const s={};for(let r=0;r<t.length;r++){const o=t[r],a=et(o);let l="";if(l=void 0===n?a:n+"."+a,a===e.textNodeName)void 0===i?i=o[a]:i+=""+o[a];else{if(void 0===a)continue;if(o[a]){let t=tt(o[a],e,l);const n=it(t,e);void 0!==o[K]&&(t[K]=o[K]),o[":@"]?nt(t,o[":@"],l,e):1!==Object.keys(t).length||void 0===t[e.textNodeName]||e.alwaysCreateTextNode?0===Object.keys(t).length&&(e.alwaysCreateTextNode?t[e.textNodeName]="":t=""):t=t[e.textNodeName],void 0!==s[a]&&s.hasOwnProperty(a)?(Array.isArray(s[a])||(s[a]=[s[a]]),s[a].push(t)):e.isArray(a,l,n)?s[a]=[t]:s[a]=t}}}return"string"==typeof i?i.length>0&&(s[e.textNodeName]=i):void 0!==i&&(s[e.textNodeName]=i),s}function et(t){const e=Object.keys(t);for(let t=0;t<e.length;t++){const n=e[t];if(":@"!==n)return n}}function nt(t,e,n,i){if(e){const s=Object.keys(e),r=s.length;for(let o=0;o<r;o++){const r=s[o];i.isArray(r,n+"."+r,!0,!0)?t[r]=[e[r]]:t[r]=e[r]}}}function it(t,e){const{textNodeName:n}=e,i=Object.keys(t).length;return 0===i||!(1!==i||!t[n]&&"boolean"!=typeof t[n]&&0!==t[n])}class st{constructor(t){this.externalEntities={},this.options=function(t){return Object.assign({},v,t)}(t)}parse(t,e){if("string"==typeof t);else{if(!t.toString)throw new Error("XML data is accepted in String or Bytes[] form.");t=t.toString()}if(e){!0===e&&(e={});const n=a(t,e);if(!0!==n)throw Error(`${n.err.msg}:${n.err.line}:${n.err.col}`)}const n=new k(this.options);n.addExternalEntities(this.externalEntities);const i=n.parseXml(t);return this.options.preserveOrder||void 0===i?i:Q(i,this.options)}addEntity(t,e){if(-1!==e.indexOf("&"))throw new Error("Entity value can't have '&'");if(-1!==t.indexOf("&")||-1!==t.indexOf(";"))throw new Error("An entity must be set without '&' and ';'. Eg. use '#xD' for '&#xD;'");if("&"===e)throw new Error("An entity with value '&' is not permitted");this.externalEntities[t]=e}static getMetaDataSymbol(){return T.getMetaDataSymbol()}}function rt(t,e){let n="";return e.format&&e.indentBy.length>0&&(n="\n"),ot(t,e,"",n)}function ot(t,e,n,i){let s="",r=!1;for(let o=0;o<t.length;o++){const a=t[o],l=at(a);if(void 0===l)continue;let u="";if(u=0===n.length?l:`${n}.${l}`,l===e.textNodeName){let t=a[l];ut(u,e)||(t=e.tagValueProcessor(l,t),t=ht(t,e)),r&&(s+=i),s+=t,r=!1;continue}if(l===e.cdataPropName){r&&(s+=i),s+=`<![CDATA[${a[l][0][e.textNodeName]}]]>`,r=!1;continue}if(l===e.commentPropName){s+=i+`\x3c!--${a[l][0][e.textNodeName]}--\x3e`,r=!0;continue}if("?"===l[0]){const t=lt(a[":@"],e),n="?xml"===l?"":i;let o=a[l][0][e.textNodeName];o=0!==o.length?" "+o:"",s+=n+`<${l}${o}${t}?>`,r=!0;continue}let h=i;""!==h&&(h+=e.indentBy);const d=i+`<${l}${lt(a[":@"],e)}`,f=ot(a[l],e,u,h);-1!==e.unpairedTags.indexOf(l)?e.suppressUnpairedNode?s+=d+">":s+=d+"/>":f&&0!==f.length||!e.suppressEmptyNode?f&&f.endsWith(">")?s+=d+`>${f}${i}</${l}>`:(s+=d+">",f&&""!==i&&(f.includes("/>")||f.includes("</"))?s+=i+e.indentBy+f+i:s+=f,s+=`</${l}>`):s+=d+"/>",r=!0}return s}function at(t){const e=Object.keys(t);for(let n=0;n<e.length;n++){const i=e[n];if(t.hasOwnProperty(i)&&":@"!==i)return i}}function lt(t,e){let n="";if(t&&!e.ignoreAttributes)for(let i in t){if(!t.hasOwnProperty(i))continue;let s=e.attributeValueProcessor(i,t[i]);s=ht(s,e),!0===s&&e.suppressBooleanAttributes?n+=` ${i.substr(e.attributeNamePrefix.length)}`:n+=` ${i.substr(e.attributeNamePrefix.length)}="${s}"`}return n}function ut(t,e){let n=(t=t.substr(0,t.length-e.textNodeName.length-1)).substr(t.lastIndexOf(".")+1);for(let i in e.stopNodes)if(e.stopNodes[i]===t||e.stopNodes[i]==="*."+n)return!0;return!1}function ht(t,e){if(t&&t.length>0&&e.processEntities)for(let n=0;n<e.entities.length;n++){const i=e.entities[n];t=t.replace(i.regex,i.val)}return t}const dt={attributeNamePrefix:"@_",attributesGroupName:!1,textNodeName:"#text",ignoreAttributes:!0,cdataPropName:!1,format:!1,indentBy:"  ",suppressEmptyNode:!1,suppressUnpairedNode:!0,suppressBooleanAttributes:!0,tagValueProcessor:function(t,e){return e},attributeValueProcessor:function(t,e){return e},preserveOrder:!1,commentPropName:!1,unpairedTags:[],entities:[{regex:new RegExp("&","g"),val:"&amp;"},{regex:new RegExp(">","g"),val:"&gt;"},{regex:new RegExp("<","g"),val:"&lt;"},{regex:new RegExp("'","g"),val:"&apos;"},{regex:new RegExp('"',"g"),val:"&quot;"}],processEntities:!0,stopNodes:[],oneListGroup:!1};function ft(t){this.options=Object.assign({},dt,t),!0===this.options.ignoreAttributes||this.options.attributesGroupName?this.isAttribute=function(){return!1}:(this.ignoreAttributesFn=_(this.options.ignoreAttributes),this.attrPrefixLen=this.options.attributeNamePrefix.length,this.isAttribute=gt),this.processTextOrObjNode=ct,this.options.format?(this.indentate=pt,this.tagEndChar=">\n",this.newLine="\n"):(this.indentate=function(){return""},this.tagEndChar=">",this.newLine="")}function ct(t,e,n,i){const s=this.j2x(t,n+1,i.concat(e));return void 0!==t[this.options.textNodeName]&&1===Object.keys(t).length?this.buildTextValNode(t[this.options.textNodeName],e,s.attrStr,n):this.buildObjectNode(s.val,e,s.attrStr,n)}function pt(t){return this.options.indentBy.repeat(t)}function gt(t){return!(!t.startsWith(this.options.attributeNamePrefix)||t===this.options.textNodeName)&&t.substr(this.attrPrefixLen)}ft.prototype.build=function(t){return this.options.preserveOrder?rt(t,this.options):(Array.isArray(t)&&this.options.arrayNodeName&&this.options.arrayNodeName.length>1&&(t={[this.options.arrayNodeName]:t}),this.j2x(t,0,[]).val)},ft.prototype.j2x=function(t,e,n){let i="",s="";const r=n.join(".");for(let o in t)if(Object.prototype.hasOwnProperty.call(t,o))if(void 0===t[o])this.isAttribute(o)&&(s+="");else if(null===t[o])this.isAttribute(o)||o===this.options.cdataPropName?s+="":"?"===o[0]?s+=this.indentate(e)+"<"+o+"?"+this.tagEndChar:s+=this.indentate(e)+"<"+o+"/"+this.tagEndChar;else if(t[o]instanceof Date)s+=this.buildTextValNode(t[o],o,"",e);else if("object"!=typeof t[o]){const n=this.isAttribute(o);if(n&&!this.ignoreAttributesFn(n,r))i+=this.buildAttrPairStr(n,""+t[o]);else if(!n)if(o===this.options.textNodeName){let e=this.options.tagValueProcessor(o,""+t[o]);s+=this.replaceEntitiesValue(e)}else s+=this.buildTextValNode(t[o],o,"",e)}else if(Array.isArray(t[o])){const i=t[o].length;let r="",a="";for(let l=0;l<i;l++){const i=t[o][l];if(void 0===i);else if(null===i)"?"===o[0]?s+=this.indentate(e)+"<"+o+"?"+this.tagEndChar:s+=this.indentate(e)+"<"+o+"/"+this.tagEndChar;else if("object"==typeof i)if(this.options.oneListGroup){const t=this.j2x(i,e+1,n.concat(o));r+=t.val,this.options.attributesGroupName&&i.hasOwnProperty(this.options.attributesGroupName)&&(a+=t.attrStr)}else r+=this.processTextOrObjNode(i,o,e,n);else if(this.options.oneListGroup){let t=this.options.tagValueProcessor(o,i);t=this.replaceEntitiesValue(t),r+=t}else r+=this.buildTextValNode(i,o,"",e)}this.options.oneListGroup&&(r=this.buildObjectNode(r,o,a,e)),s+=r}else if(this.options.attributesGroupName&&o===this.options.attributesGroupName){const e=Object.keys(t[o]),n=e.length;for(let s=0;s<n;s++)i+=this.buildAttrPairStr(e[s],""+t[o][e[s]])}else s+=this.processTextOrObjNode(t[o],o,e,n);return{attrStr:i,val:s}},ft.prototype.buildAttrPairStr=function(t,e){return e=this.options.attributeValueProcessor(t,""+e),e=this.replaceEntitiesValue(e),this.options.suppressBooleanAttributes&&"true"===e?" "+t:" "+t+'="'+e+'"'},ft.prototype.buildObjectNode=function(t,e,n,i){if(""===t)return"?"===e[0]?this.indentate(i)+"<"+e+n+"?"+this.tagEndChar:this.indentate(i)+"<"+e+n+this.closeTag(e)+this.tagEndChar;{let s="</"+e+this.tagEndChar,r="";return"?"===e[0]&&(r="?",s=""),!n&&""!==n||-1!==t.indexOf("<")?!1!==this.options.commentPropName&&e===this.options.commentPropName&&0===r.length?this.indentate(i)+`\x3c!--${t}--\x3e`+this.newLine:this.indentate(i)+"<"+e+n+r+this.tagEndChar+t+this.indentate(i)+s:this.indentate(i)+"<"+e+n+r+">"+t+s}},ft.prototype.closeTag=function(t){let e="";return-1!==this.options.unpairedTags.indexOf(t)?this.options.suppressUnpairedNode||(e="/"):e=this.options.suppressEmptyNode?"/":`></${t}`,e},ft.prototype.buildTextValNode=function(t,e,n,i){if(!1!==this.options.cdataPropName&&e===this.options.cdataPropName)return this.indentate(i)+`<![CDATA[${t}]]>`+this.newLine;if(!1!==this.options.commentPropName&&e===this.options.commentPropName)return this.indentate(i)+`\x3c!--${t}--\x3e`+this.newLine;if("?"===e[0])return this.indentate(i)+"<"+e+n+"?"+this.tagEndChar;{let s=this.options.tagValueProcessor(e,t);return s=this.replaceEntitiesValue(s),""===s?this.indentate(i)+"<"+e+n+this.closeTag(e)+this.tagEndChar:this.indentate(i)+"<"+e+n+">"+s+"</"+e+this.tagEndChar}},ft.prototype.replaceEntitiesValue=function(t){if(t&&t.length>0&&this.options.processEntities)for(let e=0;e<this.options.entities.length;e++){const n=this.options.entities[e];t=t.replace(n.regex,n.val)}return t};const mt={validate:a};module.exports=e})();

/***/ })

/******/ 	});
/************************************************************************/
/******/ 	// The module cache
/******/ 	var __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __nccwpck_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		var cachedModule = __webpack_module_cache__[moduleId];
/******/ 		if (cachedModule !== undefined) {
/******/ 			return cachedModule.exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = __webpack_module_cache__[moduleId] = {
/******/ 			// no module.id needed
/******/ 			// no module.loaded needed
/******/ 			exports: {}
/******/ 		};
/******/ 	
/******/ 		// Execute the module function
/******/ 		var threw = true;
/******/ 		try {
/******/ 			__webpack_modules__[moduleId].call(module.exports, module, module.exports, __nccwpck_require__);
/******/ 			threw = false;
/******/ 		} finally {
/******/ 			if(threw) delete __webpack_module_cache__[moduleId];
/******/ 		}
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
/******/ 	/* webpack/runtime/compat */
/******/ 	
/******/ 	if (typeof __nccwpck_require__ !== 'undefined') __nccwpck_require__.ab = __dirname + "/";
/******/ 	
/************************************************************************/
var __webpack_exports__ = {};
// This entry need to be wrapped in an IIFE because it need to be in strict mode.
(() => {
"use strict";
var exports = __webpack_exports__;

// CLI entry for APX repacker.
// Usage:
//   apx-repack --in <input> --out <result.apxtlm> [--utc <offsetSec>] [--with-jso]
Object.defineProperty(exports, "__esModule", ({ value: true }));
const command_1 = __nccwpck_require__(405);
function usage() {
    console.log(`Usage:
  apx-repack --in <path/to/input> --out <result.apxtlm> [--utc <offsetSec>] [--with-jso]

Examples:
  apx-repack --in ./sample.telemetry --out ./result.apxtlm
  apx-repack --in ./sample.datalink.xml --out ./result.apxtlm --utc 10800 --with-jso
`);
}
function parseArgv(argv) {
    const a = argv.slice(2);
    const out = { includeJso: true };
    for (let i = 0; i < a.length; i++) {
        const x = a[i];
        if (x === "-h" || x === "--help")
            out.help = true;
        else if (x === "--in")
            out.in = a[++i];
        else if (x === "--out")
            out.out = a[++i];
        else if (x === "--utc")
            out.utc = Number(a[++i] ?? 0);
    }
    return out;
}
(async () => {
    const args = parseArgv(process.argv);
    if (args.help || !args.in || !args.out) {
        usage();
        process.exit(args.help ? 0 : 1);
    }
    try {
        await (0, command_1.runRepack)({
            inFile: args.in,
            outFile: args.out,
            utcOffset: Number.isFinite(args.utc) ? args.utc : 0,
            includeJso: !!args.includeJso
        });
    }
    catch (e) {
        console.error("❌ repack error:", e?.message ?? e);
        process.exit(1);
    }
})();

})();

module.exports = __webpack_exports__;
/******/ })()
;