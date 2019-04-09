const nunjucks = require("nunjucks");
nunjucks.configure({ autoescape: true });

const countVertices = inputGraph => {
  return Object.keys(inputGraph).length;
};

const createAdjacencyList = pairs => {
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
    let x = queue.shift();
    if (graph[x]) {
      for (let i = 0; i < graph[x].length; i++) {
        let next = graph[x][i];
        let prev = level[next];
        level[next] = level[x] + 1;
        if (marked[next]) {
          level[next] = Math.max(level[next], prev);
        } else {
          marked[next] = true;
        }
        queue.unshift(next);
      }
    }
  }
  return level;
};

const calculateDirectedPairs = input => {
  let pairs = [];

  for (const node in input) {
    let value = input[node];
    if (value.inputs.length > 0) {
      for (let i = 0; i < value.inputs.length; i++) {
        pairs.push([value.inputs[i], node]);
      }
    }
  }
  return pairs;
};

const stages = (levels, filterUuids) => {
  // Create a stages array 2D with tasks based on levels calculcated with custom BFS
  let stages = {};
  let prevDepth = null;

  const uuids = filterUuids;
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
  let output = {};
  let i = 0;

  output.id = "a4250892-d9d9-49f3-aa3f-72151230f719";
  output.name = "Demo Pipeline";
  output.spaceId = "f332d24c-4eb4-40a4-9987-1a04bc315943";
  output.stages = [];

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
    FILTER: "Select * from {{inputs | first}} where {{attrs.Condition}}",
    CAST: ""
  };

  const inputs = node.inputs;

  const attrs = node.attrs;

  switch (node.type) {
    case "FILTER":
      template = sqlTemp[node.type];
      break;
  }

  const sql = nunjucks.renderString(template, {
    inputs,
    attrs
  });

  return sql;
};

const init = input => {
  const vertices = countVertices(input);
  const inputUuids = Object.keys(input).filter(
    uuid => input[uuid].type == "INPUT"
  );

  const filterUuids = Object.keys(input).filter(
    uuid => input[uuid].type == "FILTER"
  );
  const pairs = calculateDirectedPairs(input);
  const graph = createAdjacencyList(pairs);
  const levels = bfs(graph, vertices, inputUuids);
  const stageCollection = stages(levels, filterUuids);
  // const output = createTemplatizedSqlOutput(input, stageCollection);
  console.log(stageCollection);
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
    id: "Cars",
    type: "INPUT",
    attrs: {},
    inputs: []
  },
  {
    id: "2",
    type: "FILTER",
    attrs: {
      Condition: "make = Chevrolet"
    },
    inputs: ["Cars"]
  },
  {
    id: "3",
    type: "OUTPUT",
    attrs: {},
    inputs: [2]
  }
];

var input3 = [
  {
    id: "42f01268-de1a-41fb-841a-218b503dceee",
    type: "INPUT",
    inputs: [],
    attrs: {
      table_name: "Cars"
    }
  },
  {
    id: "996db6fc-395b-46d5-85e1-54ab0984055f",
    type: "FILTER",
    inputs: ["42f01268-de1a-41fb-841a-218b503dceee"],
    attrs: {
      Condition: "make = Chevrolet"
    }
  },
  {
    id: "4619ddd8-f7c8-4020-8659-4a6ec2a2413b",
    type: "OUTPUT",
    inputs: ["996db6fc-395b-46d5-85e1-54ab0984055f"]
  },
  {
    id: "996db6fc-395b-46d5-85e1-54ab0984056g",
    type: "FILTER",
    inputs: ["42f01268-de1a-41fb-841a-218b503dceee"],
    attrs: {
      Condition: "make = Suzuki"
    }
  },
  {
    id: "996db6fc-395b-46d5-85e1-54ab0984056t",
    type: "OUTPUT",
    inputs: ["996db6fc-395b-46d5-85e1-54ab0984056g"]
  }
];

var payload = [
  {
    id: "42f01268-de1a-41fb-841a-218b503dceee",
    type: "INPUT",
    inputs: [],
    attrs: {
      table_name: "Cars"
    }
  },
  {
    id: "996db6fc-395b-46d5-85e1-54ab0984055f",
    type: "FILTER",
    inputs: ["42f01268-de1a-41fb-841a-218b503dceee"],
    attrs: {
      Condition: "make = Chevrolet"
    }
  },
  {
    id: "4619ddd8-f7c8-4020-8659-4a6ec2a2413b",
    type: "OUTPUT",
    inputs: ["996db6fc-395b-46d5-85e1-54ab0984055f"]
  },
  {
    id: "996db6fc-395b-46d5-85e1-54ab0984056g",
    type: "FILTER",
    inputs: ["42f01268-de1a-41fb-841a-218b503dceee"],
    attrs: {
      Condition: "make = Suzuki"
    }
  },
  {
    id: "996db6fc-395b-46d5-85e1-54ab0984056t",
    type: "OUTPUT",
    inputs: ["996db6fc-395b-46d5-85e1-54ab0984056g"]
  }
];

var revisedInput = {
  "a32f4c87-ae80-4296-a0b6-1551e0d3c741": {
    type: "INPUT",
    attrs: {
      table_name: "Cars"
    },
    inputs: []
  },
  "063b7040-d502-45d5-9bdf-49af2420095a": {
    type: "OUTPUT",
    attrs: {
      Result: "Output store"
    },
    inputs: ["9b80bd77-e446-417c-b1db-b0db6c6732c8"]
  },
  "f091f096-0ca8-4e6f-b6da-7e30e4dfe1ea": {
    type: "FILTER",
    attrs: {
      Condition: "make = Dodge",
      table_name: "Cars"
    },
    inputs: ["a32f4c87-ae80-4296-a0b6-1551e0d3c741"]
  },
  "9b80bd77-e446-417c-b1db-b0db6c6732c8": {
    type: "FILTER",
    attrs: {
      Condition: "make = Honda",
      table_name: "Cars"
    },
    inputs: ["f091f096-0ca8-4e6f-b6da-7e30e4dfe1ea"]
  }
};

init(revisedInput);
