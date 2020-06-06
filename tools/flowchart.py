import re
from pprint import pprint
from collections import OrderedDict
from fire import Fire
from graphviz import Digraph

LINE0 = '<BO(SS=131,N_CC=6,N_PC=8,N_SC=12,N_SF=[51],LSN=2)> | DONE | OK'
# LINE1= '<BO(SS=145,N_CC=7,N_PC=9,N_SC=13,N_SE=12,N_SF=[52],LSN=3)> | DONE | OK'
LINE1= '<NSO(SS=145,N_CC=7,N_PC=9,N_SC=13,N_SE=12,N_SF=[52],LSN=3)> | DONE | OK'
LINE2 = '<MO(SS=161,N_CC=8,N_PC=10,N_SC=14,N_SE=13,S_SE=[1:12],N_SF=[53],LSN=4)> | DONE | OK'
LINE3 = '<BO(SS=174,,N_SF=[54],LSN=6)> | DONE | Error code(70003): Specified snapshot holder not found'
LINE4 = '<MO(SS=161,N_CC=8,N_PC=10,N_SC=14,N_SE=13,S_SE=[1:12],N_SF=[53],LSN=8)> | DONE | OK'
# LINE = '<CCO(CID=0,CNAME="c1",N_CC=1,N_CID=1,N_CNAME="c1",N_PC=1,N_PID=1,N_PNAME="_default",LSN=2)> | DONE | OK'
LINES = [LINE0, LINE1, LINE2, LINE3, LINE4]

class Parser:
    def parse_lines(self, lines):
        nodes = OrderedDict()
        for line in lines:
            lsn, node = self.parse_line(line)
            if not lsn:
                continue
            nodes[lsn] = node
        return nodes

    def parse_line(self, line):
        if not line:
            return None, {}
        lines = list(map(str.strip, line.split('|')))
        kvs = OrderedDict()
        kvs['node'] = self._parse_node(lines[0])
        kvs['lsn'] = kvs['node'].pop('lsn')
        kvs['state'] = lines[1]
        if len(lines) == 3:
            health = lines[2]
            kvs['health'] = self._parse_health(health)

        return kvs['lsn'], kvs

    def _parse_health(self, line):
        if not line:
            return {}
        r = OrderedDict()
        if line.startswith('OK'):
            r['status'] = 'OK'
        elif line.startswith('Error'):
            r['status'] = 'ERR'
            lines = list(map(str.strip, line.split(':')))
            assert len(lines) == 2, f'Invalid health info: {line}'
            m = re.match(r'Error code\((\w+)\)', lines[0])
            if not m:
                raise RuntimeError(f'Invalid health info: {line}')
            error_code = m.group(1)
            r['error_code'] = m.group(1)
            r['error_msg'] = lines[1]
        return r

    def _parse_node(self, line):
        if not line or not line.startswith('<') or not line.endswith('>'):
            raise RuntimeError(f'Invalid node info: {line}')
        r = OrderedDict()
        line = line[1:-1]
        m = re.match(r'(\w+)\((.*)\)', line)
        if not m:
            raise RuntimeError(f'Invalid node info: {line}')
        op = m.group(1)
        action_items = [s for s in map(str.strip, m.group(2).split(',')) if s]
        actions = OrderedDict()

        for item in action_items:
            tt = item.split('=')
            if tt[0] == 'LSN':
                r['lsn'] = int(tt[1])
                continue
            actions[tt[0]] = tt[1]

        r['name'] = op
        r['actions'] = actions
        return r

def create_start_node(g):
    start_id = '0'
    n = g.node(start_id, 'Start', shape='doublecircle')
    return start_id

def create_end_node(g):
    node_id = '-1'
    n = g.node(node_id, 'End', shape='doublecircle')
    return node_id

def create_node(g, node_id, node_info):
    node_name = node_info['node']['name']
    attrs = {}
    if node_info['health']['status'] != 'OK':
        attrs['color'] = 'pink'
        error_code = node_info['health']['error_code']
        node_name = f'{node_name}:<{error_code}>'
    g.node(node_id, node_name, **attrs)
    return node_id

class Flow:
    def __init__(self, name, nodes, **kwargs):
        self.name = name
        self.nodes = nodes
        self.kwargs = kwargs

    def draw_cluster(self):
        fmt = self.kwargs.get('format', 'svg')
        g = Digraph(self.name, filename='cluster.gv')
        # g.node_attr.update(style='filled', color='lightblue')
        points = {}
        with g.subgraph(name='cluster_0') as bo:
            bo.attr(style='filled', color='lightgrey')
            # bo.attr(color='blue')
            bo.node_attr.update(style='filled', color='white')
            bo_points = []
            for lsn, node in self.nodes.items():
                node_id = str(lsn)
                if node['node']['name'] != 'BO':
                    continue
                points[lsn] = node_id
                bo_points.append(node_id)

            bo_edges = [[bo_points[0]]]
            for point in bo_points[1:]:
                edge = bo_edges[-1]
                edge.append(point)
                bo_edges.append([point])

            bo_edges = [edge for edge in bo_edges if len(edge) == 2]
            bo.edges(bo_edges)
            bo.attr(label='Build Op')

        with g.subgraph(name='cluster_1') as mo:
            mo.attr(style='filled', color='lightblue')
            mo.node_attr.update(style='filled', color='white')
            mo_points = []
            for lsn, node in self.nodes.items():
                node_id = str(lsn)
                if node['node']['name'] != 'MO':
                    continue
                points[lsn] = node_id
                mo_points.append(node_id)
            mo_edges = [[mo_points[0]]]
            for point in mo_points[1:]:
                edge = mo_edges[-1]
                edge.append(point)
                mo_edges.append([point])

            mo_edges = [edge for edge in mo_edges if len(edge) == 2]
            mo.edges(mo_edges)
            mo.attr(label='Merge Op')

        start_id = create_start_node(g)
        end_id = create_end_node(g)
        edges = [[start_id]]
        pprint(points)
        # for lsn, point in points.items():
        for lsn in sorted(points.keys()):
            print(lsn)
            edge = edges[-1]
            edge.append(points[lsn])
            edges.append([points[lsn]])

        edge = edges[-1]
        edge.append(end_id)

        pprint(edges)
        for edge in edges:
            if len(edge) != 2:
                continue
            g.edge(*edge)



        # g.edge(start_id, '2')
        # g.edge(start_id, '4')
        # g.edge('2', '8')
        # g.edge('4', '6')
        # g.edge('6', '-1')
        # g.edge('8', '-1')

        g.view()

    def draw(self):
        fmt = self.kwargs.get('format', 'svg')
        # engine: fdp, sfdp, neato
        dot = Digraph(comment=self.name, format=fmt, engine='sfdp')
        dot.attr(color='lightdark', style='filled', size='10,10')
        dot.node_attr.update(style='filled', color='lightblue')
        start_id = create_start_node(dot)
        edges = [[start_id]]
        for lsn, node in self.nodes.items():
            node_id = str(lsn)
            create_node(dot, node_id, node)
            if len(edges) == 0:
                edge = [node_id]
                edges.append(edge)
            else:
                edge = edges[-1]
                edge.append(node_id)
                edges.append([node_id])

        end_id = create_end_node(dot)
        edges[-1].append(end_id)

        for edge in edges:
            if len(edge) != 2:
                continue
            dot.edge(*edge)


        dot.view()

if __name__ == '__main__':
    p = Parser()
    nodes = p.parse_lines(LINES)
    f = Flow('sample', nodes)
    f.draw_cluster()
