import download from "download";
import * as fs from "fs";
import { parse, parseFile } from "fast-csv";

const CSV_PATH =
  "https://gist.githubusercontent.com/bobbae/b4eec5b5cb0263e7e3e63a6806d045f2/raw/279b794a834a62dc108fc843a72c94c49361b501/data.csv";
const CSV_NAME = "data.csv";

async function all() {
  if (fs.existsSync("data2.json")) {
    fs.unlink("data2.json", (err) => {
      if (err) {
        console.error(err);
      }
    });
  }
  if (fs.existsSync("formatted.csv")) {
    fs.unlink("formatted.csv", (err) => {
      if (err) {
        console.error(err);
      }
    });
  }
  fs.writeFileSync("data2.json", "");
  fs.writeFileSync("formatted.csv", "");
  await download(CSV_PATH, ".");

  const jsonObject = {};
  let validRows = 0;

  const writeStream = fs.createWriteStream("formatted.csv", {
    flags: "a",
  });
  const data2JsonWriteStream = fs.createWriteStream("data2.json", {
    flags: "a",
  });

  parseFile(CSV_NAME)
    .on("error", (error) => console.error(error))
    .on("data", () => {})
    .on("end", (rowCount) => {
      console.log(`TOTAL ROWS: ${rowCount}`);
      parseFile(CSV_NAME)
        .on("error", (error) => console.error(error))
        .on("data", (row) => {
          if (row && Array.isArray(row)) {
            const headerRow = row.includes("Year");

            let firstRow = row;
            if (headerRow) {
              firstRow[3] = "Revenue";
              firstRow[4] = "Profit";

              firstRow.forEach((title) => {
                jsonObject[title] = [];
              });
            }

            if (!isNaN(row[4]) || headerRow) {
              validRows += 1;
              writeStream.write(
                headerRow ? `${firstRow.join(",")}\n` : `${row.join(",")}\n`
              );
            }
          }
        })
        .on("end", async (rowCount) => {
          console.log(`TOTAL VALID PROFIT ROWS: ${validRows}`);
          parseFile("formatted.csv")
            .on("error", (error) => console.error(error))
            .on("data", (row) => {
              const headerRow = row.includes("Year");
              if (!headerRow) {
                jsonObject["Year"] = [...jsonObject["Year"], row[0]];
                jsonObject["Rank"] = [...jsonObject["Rank"], row[1]];
                jsonObject["Company"] = [...jsonObject["Company"], row[2]];
                jsonObject["Revenue"] = [...jsonObject["Revenue"], row[3]];
                jsonObject["Profit"] = [...jsonObject["Profit"], row[4]];
              }
            })
            .on("end", async (rowCount) => {
              console.log("jsonObject ", jsonObject);
              data2JsonWriteStream.write(JSON.stringify(jsonObject));
              console.log(
                `Valid rows after removing non numrical profit: ${validRows}`
              );
            });
        });
    });
}

all();
