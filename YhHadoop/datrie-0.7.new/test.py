#!/usr/bin/python
# -*- coding: utf-8 -*-

import datrie
import string
#trie = datrie.Trie(string.ascii_lowercase)
#trie = datrie.Trie(ranges=[(0x4e00, 0x9fa5)])
trie = datrie.Trie(ranges=[
  # Chineese
  (unichr(0x4e00), unichr(0x9fa5)),
  # Number
  (chr(0x0030), chr(0x0039)),
  # Alphabet
  (chr(0x0041), chr(0x005a)),
  (chr(0x0061), chr(0x007a))
])
trie[u'foo'] = 5
trie[u'123'] = 6
#print trie.__setitem__(u'foo', 5)
trie[u'中国'] = 7
trie[u'中国1'] = 7
trie[u'中国a'] = 7
trie[u'中国测试'] = 7
#print trie.__setitem__(u'中国', 5)
#trie[u'foo']
print trie[u'foo']
print trie[u'123']
print trie[u'中国']
print trie.prefixes(u'中国aaaaaaaa')
print trie.prefix_items(u'中国aaaaaaaa')
print trie.keys(u'中国')
