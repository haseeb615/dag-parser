const nunjucks = require("nunjucks");
nunjucks.configure({ autoescape: true });

const countVertices = inputGraph => {
  return inputGraph.length;
};

const createAdjacencyList = pairs => {
  // Create 2D array
  //convert the index of array to be uuid
  let graph = [];
  for (let i = 0; i < pairs.length; i++) {
    if (graph[pairs[i][0]] == undefined) {
      graph[pairs[i][0]] = [];
    }
    graph[pairs[i][0]].push(pairs[i][1]);
  }
  return graph;
};

const bfs = (graph, V, uuids) => {
  let level = {};
  let marked = new Array(V);
  let queue = [];

  uuids.map(uuid => {
    level[uuid] = 0;
    queue.push(uuid);
    marked[uuid] = true;
  });

  while (queue.length != 0) {
    x = queue.shift();
    if (graph[x]) {
      for (let i = 0; i < graph[x].length; i++) {
        let next = graph[x][i];
        if (marked[next]) {
          prev = level[next];
          level[next] = level[x] + 1;
          level[next] = Math.max(level[next], prev);
        } else {
          queue.unshift(next);
          level[next] = level[x] + 1;
          marked[next] = true;
        }
      }
    }
  }
  return level;
};

const calculateDirectedPairs = input => {
  let pairs = [];
  // TODO change this to simple for loop
  input.map(node => {
    if (node.inputs.length > 0) {
      for (let i = 0; i < node.inputs.length; i++) {
        pairs.push([node.inputs[i], node.id]);
      }
    }
  });
  return pairs;
};

const stages = (levels, input) => {
  // Create a stages array 2D with tasks based on levels calculcated with custom BFS
  let stages = {};
  let prevDepth = null;
  const operations = input.filter(node => node.type == "FILTER");
  const uuids = operations.map(node => node.id);
  for (const key in levels) {
    let uuid = key;
    let depth = levels[key];
    if (depth == prevDepth) {
      stages[depth].push(uuid);
      continue;
    }

    if (depth > 0 && uuids.includes(uuid)) {
      prevDepth = depth;
      stages[depth] = [uuid];
    }
  }
  return stages;
};

const findNodeByUUID = (input, uuid) => {
  let node = input.filter(node => node.id == uuid);
  return node[0];
};

const createTemplatizedSqlOutput = (input, stages) => {
  // Refactor this method with string interpolation
  // make sql_template a separate function and creation of JSON as a separate func
  let output = {};
  let i = 0;

  output.id = "a4250892-d9d9-49f3-aa3f-72151230f719";
  output.name = "Demo Pipeline";
  output.spaceId = "f332d24c-4eb4-40a4-9987-1a04bc315943";
  output.stages = [];

  const sqlTemp = {
    filter: ["Select * from $", "where $"]
  };

  for (const stage in stages) {
    output.stages[i] = {};
    output.stages[i].name = "Stage1";
    output.stages[i].tasks = [];
    console.log(stages);

    for (let j = 0; j < stages[stage].length; j++) {
      let task = {
        name: "Task1",
        type: "SQL",
        config: {},
        public: true,
        target: "52f33803-23cf-4767-a6af-285633acb6a8"
      };

      const node = findNodeByUUID(input, stages[stage][j]);
      // const inputSource = findNodeByUUID(input, node.inputs[0]).attrs[
      //   "table_name"
      // ];
      // const condition = node.attrs["Condition"];
      // const sql =
      //   sqlTemp["filter"][0].split("$").join(inputSource) +
      //   " " +
      //   sqlTemp["filter"][1].split("$").join(condition);
      // task.sql = sql;

      task.sql = sqlTemplate(input, node);

      output.stages[i].tasks[j] = task;
    }
    i++;
  }
  return output;
};

const sqlTemplate = (input, node) => {
  let template = "";
  const sqlTemp = {
    FILTER: "Select * from {{inputSource}} where {{condition}}",
    JOIN: ""
  };

  // For single input operations not for Joins
  const inputSource = findNodeByUUID(input, node.inputs[0]).attrs["table_name"];

  const condition = node.attrs["Condition"];

  switch (node.type) {
    case "FILTER":
      template = sqlTemp[node.type];
      break;
  }

  const sql = nunjucks.renderString(template, {
    inputSource,
    condition
  });

  return sql;
};

const init = input => {
  //TODO
  // Convert graph in bfs to a hash object
  const vertices = countVertices(input);
  const totalInputs = input.filter(node => node.type == "INPUT");
  const uuid = totalInputs[0].id;
  const uuids = totalInputs.map(input => input.id);
  const pairs = calculateDirectedPairs(input);
  const graph = createAdjacencyList(pairs);
  const levels = bfs(graph, vertices, uuids);
  const stageCollection = stages(levels, input);
  const output = createTemplatizedSqlOutput(input, stageCollection);
  // console.log(JSON.stringify(uuids));
  // console.log(Math.max(2, 3));
  console.log(stageCollection);
  console.log(JSON.stringify(output));

  // TODO
  // Incorporate multiple inputs
  // calculate maximum depth
  // Add Join of two tables for start later move it to 3.
  // console.log(JSON.stringify(output));
};

var originalInput = [
  {
    id: "a",
    type: "INPUT",
    inputs: [],
    attrs: {
      table_name: "Cars"
    }
  },
  {
    id: "b",
    type: "INPUT",
    inputs: [],
    attrs: {
      table_name: "Glasses"
    }
  },
  {
    id: "c",
    type: "FILTER",
    inputs: ["a", "f"],
    attrs: {
      Condition: "make = Chevrolet"
    }
  },
  {
    id: "d",
    type: "OUTPUT",
    inputs: ["c"]
  },
  {
    id: "e",
    type: "FILTER",
    inputs: ["a"],
    attrs: {
      Condition: "make = Suzuki"
    }
  },
  {
    id: "g",
    type: "OUTPUT",
    inputs: ["e"]
  },
  {
    id: "f",
    type: "FILTER",
    inputs: ["b"],
    attrs: {
      Condition: "make = Honda"
    }
  }
];

var input = [
  {
    id: "1",
    type: "INPUT",
    attrs: {
      table_name: "Cars"
    },
    inputs: []
  },
  {
    id: "2",
    type: "FILTER",
    attrs: {
      Condition: "make = Chevrolet"
    },
    inputs: [1]
  },
  {
    id: "3",
    type: "OUTPUT",
    attrs: {},
    inputs: [2]
  }
];

// var originalInput3 = [
//   {
//     id: 1,
//     type: "INPUT",
//     inputs: [],
//     attrs: [
//       {
//         table_name: "Cars"
//       }
//     ]
//   },
//   {
//     id: 2,
//     type: "FILTER",
//     inputs: [1],
//     attrs: [
//       {
//         condition: "make = Chevrolet"
//       }
//     ]
//   },
//   {
//     id: 3,
//     type: "OUTPUT",
//     inputs: [2]
//   }
// ];

init(input);
