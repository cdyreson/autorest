/** Taken from "The Definitive ANTLR 4 Reference" by Terence Parr */

// Derived from http://json.org
grammar JSON;

@header {
package autowebservices.grammar;
import java.util.*;
}

@members {
private Visitor v;
  
}

json
   :    { 
          v.enterJson();
        }
     value 
        { 
          v.exitJson();
        }
   ;

obj
   : '{' 
        {
          v.enterObj();
        }
     pair (',' pair)*
        {
          v.exitObj();
        }
     '}'
   | '{' '}'
   ;

pair
   : STRING ':' 
        { 
          v.enterPair($STRING.text);
        }
     value 
        {
          v.exitPair();
        }
   ;

array
   : '[' 
        { 
          v.enterArray(); 
        } 
     value (',' value)* ']'
        {
          v.exitArray();
        }
   | '[' ']'
   ;

value
   : STRING 
        {
          v.parsedString($STRING.text);
        }
   | NUMBER
   | obj
   | array
   | 'true'
   | 'false'
   | 'null'
   ;


STRING
   : '"' (ESC | SAFECODEPOINT)* '"'
   ;


fragment ESC
   : '\\' (["\\/bfnrt] | UNICODE)
   ;


fragment UNICODE
   : 'u' HEX HEX HEX HEX
   ;


fragment HEX
   : [0-9a-fA-F] 
   ;


fragment SAFECODEPOINT
   : ~ ["\\\u0000-\u001F]
   ;


NUMBER
   : '-'? INT ('.' [0-9] +)? EXP?
   ;


fragment INT
   : '0' | [1-9] [0-9]*
   ;

// no leading zeros

fragment EXP
   : [Ee] [+\-]? INT
   ;

// \- since - means "range" inside [...]

WS
   : [ \t\n\r] + -> skip
   ;
