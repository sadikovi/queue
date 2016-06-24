B = require '../coffee/z'

describe 'Queue tests', ->
  it 'checks tests', ->
    expect("test").toEqual 'test'


  it 'should import', ->
    b = new B()
    expect(b.name).toEqual 'b'
