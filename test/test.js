const {initialFunction} = require("../index")

const chai = require("chai")
const chaiAsPromised = require("chai-as-promised")
chai.use(chaiAsPromised)
chai.should()

describe("Module", () => {
    it("should return a function", done => {
        initialFunction.should.be.a("function")
        done()
    })
})