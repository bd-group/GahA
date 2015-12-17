$(display_all);

var config = {
    //linLogMode: true,
	//outboundAttractionDistribution:false,
	adjustSizes:true,
	edgeWeightInfluence:1,
	//scalingRatio :1,
	strongGravityMode:true,
	gravity:1,
	barnesHutOptimize:true,
	barnesHutTheta:0.5,
	//slowDown:1,
	//startingIterations:1,
	iterationsPerRender:1,
	worker:true, 
}

function display_all() {
    // Add a method to the graph model that returns an
    // object with every neighbors of a node inside:
    sigma.classes.graph.addMethod('neighbors', function(nodeId) {
        var k,
        neighbors = {},
        index = this.allNeighborsIndex[nodeId] || {};

        for (k in index)
            neighbors[k] = this.nodesIndex[k];

        return neighbors;
    });
    
    var g = { nodes: [], edges: [] };
    s = new sigma({
        graph: g,
        renderer: {
	        container: document.getElementById('container'),
            type: 'webgl',
	    },
	    settings: { 
		    hideEdgesOnMove: true,
		    minNodeSize: 1,
		    maxNodeSize: 10, 
		    minEdgeSize: 1,
		    maxEdgeSize: 5,
		    batchEdgesDrawing: false,
            enableEdgeHovering: false,
            defaultEdgeType: 'arrow',
            defaultLabelColor: '#FFF',
            minArrowSize: 1,
		    edgeHoverSizeRatio: 2
	    }
    });

    design = sigma.plugins.design(s, {
        palette: myPalette,
        styles: myStyles
    });

    s.bind('clickNode', function(e) {
        var nodeId = e.data.node.id;

        if (gw2 == null && gw.nodes(nodeId) != null) {
            gw2 = new sigma.classes.graph();

            s.graph.nodes().forEach(function(n) {
                gw2.addNode(n);
            });
            s.graph.edges().forEach(function(edge) {
                gw2.addEdge(edge);
            });
            s.killForceAtlas2();
            s.graph.clear();

            s.graph.addNode({
                id: nodeId,
                label: 'F' + nodeId,
                x: Math.random(),
                y: Math.random(),
                size: gw.degree(nodeId, "in"),
                color: gw.nodes(nodeId).color,
                type: 'star',
                star: {
                    innerRatio: 0.4 + Math.random() * 0.2,
                    numPoints: 5,
                },
            });
            var maxsize = 1;
            gw.edges().forEach(function(edge) {
                if (edge.source == nodeId) {
                    if (gw.degree(edge.target, "in") > 0) {
                        if (s.graph.nodes(edge.target) == null)
                            s.graph.addNode({
                                id: edge.target,
                                label: 'F' + edge.target,
                                x: Math.random(),
                                y: Math.random(),
                                size: gw.degree(edge.target, "in"),
                                color: gw.nodes(edge.target).color,
                            });
                        s.graph.addEdge({
                            id: edge.id,
                            source: edge.source,
                            target: edge.target,
                            color: '#383838',
                            weight: gw.degree(edge.target, "in"),
                            type: 'arrow',
                        });
                        if (maxsize < gw.degree(edge.target, "in"))
                            maxsize = gw.degree(edge.target, "in");
                    }
                }
            });
            s.graph.nodes(nodeId).size = maxsize;
            s.refresh();

            s.startForceAtlas2(config);

            design.deprecate('nodes');
            design.apply("nodes");
        }
    });

    s.bind('clickStage', function(e) {
        if (gw2 != null) {
            s.killForceAtlas2();
            s.graph.clear();

            gw2.nodes().forEach(function(n) {
                s.graph.addNode(n);
            });
            gw2.edges().forEach(function(edge) {
                s.graph.addEdge(edge);
            });

            s.refresh();
            gw2.clear();
            gw2 = null;
        }
    });

    gw = new sigma.classes.graph();
    gw2 = null;

    porstate = 0;
}
    
function qfact() {
    var factid = document.getElementById("factid").value;

 	$.getJSON(window.location.protocol + "//" + window.location.host + 
              '/gaha/qfact?factid=' + factid + '&callback=?', function(data) {
                  json = eval(data);
                  
                  s.killForceAtlas2();

                  $(json.nodes).each(function(index, item){
                      var nodecolor = '#' + Math.floor(Math.random()*16777215).toString(16);

                      if (s.graph.nodes('' + item) == null) {
                          s.graph.addNode({ 
						      id: '' + item,
						      label: 'F' + item,
						      x: Math.random(),
						      y: Math.random(),
						      size: 1,
						      originalColor: nodecolor,
						      color: nodecolor, 
					      });
                      }
                  });
                  
                  for(var index in json.edges){
                      var color = 'rgba(0,10,0,0)';

                      s.graph.nodes().forEach(function(n) {
						  if ('' + index == n.id){
							  color = n.color;
						  }
					  });

                      $(json.edges[index]).each(function(stauts, edgetarget){
                          if (s.graph.edges(index + '@' + edgetarget) == null) {
                              s.graph.addEdge({
							      id: index + '@' + edgetarget,
							      source: '' + index,
							      target: '' + edgetarget, 
							      originalColor: color,
							      color: color,	
						      });
                          }
					  });  
                  }
                  s.refresh();	
     			  s.startForceAtlas2(config);
              });

    var tr = setTimeout(function() {
        //s.stopForceAtlas2();
        clearTimeout(tr);
    }, 10000);
}

function qfacts() {
    var factid = document.getElementById("factid").value;

 	$.getJSON(window.location.protocol + "//" + window.location.host + 
              '/gaha/qfacts?factid=' + factid + '&callback=?', function(data) {
                  json = eval(data);
                  
                  s.killForceAtlas2();

                  $(json.nodes).each(function(index, item){
                      var nodecolor = '#' + Math.floor(Math.random()*16777215).toString(16);

                      if (s.graph.nodes('' + item) == null) {
                          s.graph.addNode({ 
						      id: '' + item,
						      label: 'F' + item,
						      x: Math.random(),
						      y: Math.random(),
						      size: 1,
						      originalColor: nodecolor,
						      color: nodecolor, 
					      });
                      }
                  });
                  
                  for(var index in json.edges){
                      var color = 'rgba(0,10,0,0)';

                      s.graph.nodes().forEach(function(n) {
						  if ('' + index == n.id){
							  color = n.color;
						  }
					  });

                      $(json.edges[index]).each(function(stauts, edgetarget){
                          if (s.graph.edges(index + '@' + edgetarget) == null) {
                              s.graph.addEdge({
							      id: index + '@' + edgetarget,
							      source: '' + index,
							      target: '' + edgetarget, 
							      originalColor: color,
							      color: color,	
						      });
                          }
					  });  
                  }
                  s.refresh();
     			  s.startForceAtlas2(config);
              });

    var tr = setTimeout(function() {
        //s.stopForceAtlas2();
        clearTimeout(tr);
    }, 10000);
}

function qfactR() {
    var factid = document.getElementById("factid").value;

 	$.getJSON(window.location.protocol + "//" + window.location.host + 
              '/gaha/qfactR?factid=' + factid + '&callback=?', function(data) {
                  json = eval(data);
                  
                  $(json.nodes).each(function(index, item) {
                      var nodecolor = '#' + Math.floor(Math.random() * 16777215).toString(16);

                      if (gw.nodes('' + item) == null) {
                          gw.addNode({ 
                              id: '' + item,
                              label: 'F' + item,
                              x: Math.random(),
                              y: Math.random(),
                              size: 1,
                              originalColor: nodecolor,
                              color: nodecolor, 
                          });
                      }
                  });
                  
                  for (var index in json.edges) {
                      var color = 'rgba(0,10,0,0)';

                      if (gw.nodes('' + index) != null)
                          color = gw.nodes('' + index).color;

                      $(json.edges[index]).each(function(stauts, edgetarget) {
                          if (gw.edges(index + '@' + edgetarget) == null) {
                              gw.addEdge({
							      id: index + '@' + edgetarget,
							      source: '' + index,
							      target: '' + edgetarget, 
							      originalColor: color,
							      color: color,	
						      });
                          }
					  });  
                  }

                  s.killForceAtlas2();
                  s.graph.clear();

                  // Generate display in sigma now
                  gw.nodes().forEach(function(n) {
                      if (gw.degree(n.id, "out") > 0 &&
                          s.graph.nodes(n.id) == null) {
                          s.graph.addNode({
                              id: n.id,
                              label: gw.degree(n.id, "out") + "-" + n.id,
                              x: n.x,
                              y: n.y,
                              size: gw.degree(n.id, "out"),
                              color: n.color,
                          });
                      }
                  });

                  s.refresh();
     			  s.startForceAtlas2(config);
              });

    var tr = setTimeout(function() {
        //s.stopForceAtlas2();
        clearTimeout(tr);
    }, 10000);
}

function pauseForceAtlas() {
    if (porstate == 0) {
        s.stopForceAtlas2();
        porstate = 1;
        document.getElementById("por").value = "Resume";
    } else {
        s.startForceAtlas2();
        porstate = 0;
        document.getElementById("por").value = "Pause";
    }
}

function effectionAnalyze() {
    var g = new sigma.classes.graph();

    s.killForceAtlas2();
    s.graph.clear();
    gw.nodes().forEach(function (n) {
        if (gw.degree(n.id, "in") > 1) {
            g.addNode({
                id: n.id,
                lable: n.lable,
                x: n.x,
                y: n.y,
                size: gw.degree(n.id, "in"),
                color: n.color,
            });
        }
    });
    alert(g.nodes().length);
    gw.edges().forEach(function (e) {
        if (g.nodes(e.target) != null && g.nodes(e.source) != null) {
            if (s.graph.nodes(e.source) == null) {
                var n = g.nodes(e.source);
                s.graph.addNode(n);
            }
            e.color = g.nodes(e.target).color;
            s.graph.addEdge({
                id: e.id,
                source: e.source,
                target: e.target,
                color: e.color,
                weight: gw.degree(e.source, "in"),
            });
        }
    });
    s.refresh();
    if (s.graph.nodes().length > 0)
        s.startForceAtlas2(config);
    g.clear();
}

var myPalette = {
  aQualitativeScheme: { 
    'A': '#7fc97f',
    'B': '#beaed4',
    'C': '#fdc086'
  },
  colorbrewer: {
    sequentialGreen: {
      3: ["#e5f5f9","#99d8c9","#2ca25f"],
      4: ["#edf8fb","#b2e2e2","#66c2a4","#238b45"],
      5: ["#edf8fb","#b2e2e2","#66c2a4","#2ca25f","#006d2c"],
      6: ["#edf8fb","#ccece6","#99d8c9","#66c2a4","#2ca25f","#006d2c"],
      7: ["#edf8fb","#ccece6","#99d8c9","#66c2a4","#41ae76","#238b45","#005824"],
      8: ["#f7fcfd","#e5f5f9","#ccece6","#99d8c9","#66c2a4","#41ae76","#238b45","#005824"],
      9: ["#f7fcfd","#e5f5f9","#ccece6","#99d8c9","#66c2a4","#41ae76","#238b45","#006d2c","#00441b"]
    }
  },
  ggplot2: {
    sequentialBlue: {
      7: ['#132b43','#1d3f5d','#27547a','#326896','#3d7fb5','#4897d4','#54aef3']
    },
  },
  aSetScheme: {
    7: ["#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00","#ffff33","#a65628"]
  },
  // see sigma.renderers.linkurious
  nodeTypeScheme: {
    'A': 'square',
    'B': 'diamond',
    'C': 'star'
  },
  // see sigma.renderers.customEdgeShapes
  edgeTypeScheme: {
    'A': 'tapered'
  },
  // see sigma.renderers.linkurious
  imageScheme: {
    'A': {
      url: 'img/img1.png',
      scale: 1.3,
      clip: 0.85
    },
    'B': {
      url: 'img/img2.png',
      scale: 1.3,
      clip: 0.85
    },
    'C': {
      url: 'img/img3.png',
      scale: 1.3,
      clip: 0.85
    }
  },
  // see sigma.renderers.linkurious
  iconScheme: {
    'A': {
      font: 'FontAwesome',
      scale: 1.0,
      color: '#fff',
      content: "\uF11b"
    },
    'B': {
      font: 'FontAwesome',
      scale: 1.0,
      color: '#fff',
      content: "\uF11c"
    },
    'C': {
      font: 'FontAwesome',
      scale: 1.0,
      color: '#fff',
      content: "\uF11d"
    }
  }
};

var myStyles = {
  nodes: {
    label: {
      by: 'id',
      format: function(value) { return '#' + value; }
    },
    color: {
        by: 'size',
        scheme: 'colorbrewer.sequentialGreen',
        bins:7
    },
  },
  edges: {
    color: {
      by: 'weight',
      scheme: 'colorbrewer.sequentialGreen',
      bins: 7
    },
  }
};
