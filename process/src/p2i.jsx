import React, { useState, useEffect, useCallback, useRef } from 'react';
import ReactFlow, {
  Background, Controls, MiniMap,
  useNodesState, useEdgesState,
  MarkerType, Position,
  getBezierPath,
  EdgeLabelRenderer, BaseEdge, Handle,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  BarChart, Bar, ComposedChart, Line, PieChart, Pie,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell, Legend
} from 'recharts';
import './App.css';

const API = 'http://localhost:8000';

const C = {
  blue700:'#0078D4', teal:'#038387', red:'#D13438', purple:'#5C2D91',
  slate:'#605E5C', bg:'#F0F2F5', card:'#FFFFFF', border:'#E1DFDD',
  orange:'#CA5010', green:'#107C10', selected:'#EFF6FF', selectedBorder:'#0078D4',
  headerBg:'#1B2A4A', mapNodeBg: '#A5C6D9', mapNodeBorder: '#648596', mapEdge: '#8B9CB3',
  jkBlue: '#0057B7', yellow: '#FFF4CE'
};

const ACCENT=['#0078D4','#038387','#CA5010','#D13438','#5C2D91','#E3008C',
  '#00B7C3','#107C10','#F59E0B','#4F6BED','#E81123','#8B5CF6','#84CC16'];

/* ─── LOADING OVERLAY WITH PHASES ────────────────────────────────────────── */
const LoadingOverlay=({visible,progress,label})=>{
  if(!visible) return null;

  let activeStep = 1;
  if (progress > 10 && progress < 100) activeStep = 2;
  if (progress >= 100) activeStep = 3;

  const phases = [
    { num: 1, name: 'Processing Data' },
    { num: 2, name: 'Analysing Data' },
    { num: 3, name: 'Dashboard Created' }
  ];

  return(
    <div style={{position:'fixed',inset:0,zIndex:99999,
      background:'rgba(27,42,74,0.92)',backdropFilter:'blur(8px)',
      display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',gap:36}}>
      <style>{`@keyframes lo-pulse{0%,100%{opacity:1}50%{opacity:.45}}`}</style>
      <img src="/logo.png" alt="AJALabs Logo" style={{height:80,objectFit:'contain',animation:'lo-pulse 1.5s ease-in-out infinite'}}/>
      <div style={{display:'flex',gap:40,alignItems:'center'}}>
        {phases.map((phase,index)=>{
          const isActive=activeStep===phase.num;
          const isDone=activeStep>phase.num;
          const color=isActive||isDone?'#00B7C3':'rgba(255,255,255,0.25)';
          return(
            <div key={phase.num} style={{display:'flex',flexDirection:'column',alignItems:'center',gap:10,position:'relative'}}>
              {index>0&&(<div style={{position:'absolute',right:'100%',top:16,width:40,height:2,background:isDone||isActive?'#00B7C3':'rgba(255,255,255,0.15)',marginRight:10,transition:'all 0.4s ease'}}/>)}
              <div style={{width:32,height:32,borderRadius:'50%',background:isDone?'#00B7C3':(isActive?'rgba(0,183,195,0.1)':'transparent'),border:`2px solid ${color}`,display:'flex',alignItems:'center',justifyContent:'center',color:isDone?'#1B2A4A':color,fontWeight:'bold',fontSize:14,zIndex:2,transition:'all 0.3s ease',boxShadow:isActive?'0 0 12px rgba(0,183,195,0.4)':'none'}}>
                {isDone?'✓':phase.num}
              </div>
              <div style={{color:isActive||isDone?'#fff':'rgba(255,255,255,0.4)',fontSize:13,fontWeight:isActive?700:500,transition:'all 0.3s ease',letterSpacing:0.5}}>{phase.name}</div>
            </div>
          );
        })}
      </div>
      <div style={{display:'flex',flexDirection:'column',alignItems:'center',gap:8,width:400}}>
        <div style={{width:'100%',background:'rgba(255,255,255,.15)',borderRadius:8,height:6,overflow:'hidden'}}>
          <div style={{height:'100%',borderRadius:8,transition:'width .4s ease',background:'linear-gradient(90deg,#0078D4,#00B7C3)',width:`${progress}%`,boxShadow:'0 0 12px rgba(0,120,212,.6)'}}/>
        </div>
        <div style={{fontSize:12,color:'rgba(255,255,255,.6)'}}>{label}</div>
      </div>
    </div>
  );
};

/* ─── CUSTOM FLOW ELEMENTS ─── */

// Helper: cubic bezier point for sweep edges
const cubicBezierPt=(p0,p1,p2,p3,t)=>{const mt=1-t;return mt*mt*mt*p0+3*mt*mt*t*p1+3*mt*t*t*p2+t*t*t*p3;};

const FreqEdge=React.memo(({id,sourceX,sourceY,targetX,targetY,sourcePosition,targetPosition,data,markerEnd,style})=>{
  const sweepSide=data?.sweepSide;
  const sweepDist=data?.sweepDist??120;
  const curvature=data?.curvature??0.5;
  const freq=data?.value||data?.frequency||0;
  const maxF=data?.maxFreq||1;
  const width=1+(freq/maxF)*4;
  const arcColor='#605E5C';

  let edgePath,labelX,labelY;

  if(sweepSide){
    let cx1,cy1,cx2,cy2;
    if(sweepSide==='right'){cx1=sourceX+sweepDist;cy1=sourceY;cx2=targetX+sweepDist;cy2=targetY;}
    else if(sweepSide==='left'){cx1=sourceX-sweepDist;cy1=sourceY;cx2=targetX-sweepDist;cy2=targetY;}
    else if(sweepSide==='top'){cx1=sourceX;cy1=sourceY-sweepDist;cx2=targetX;cy2=targetY-sweepDist;}
    else{cx1=sourceX;cy1=sourceY+sweepDist;cx2=targetX;cy2=targetY+sweepDist;}
    edgePath=`M ${sourceX} ${sourceY} C ${cx1} ${cy1}, ${cx2} ${cy2}, ${targetX} ${targetY}`;
    const t=0.65;
    labelX=cubicBezierPt(sourceX,cx1,cx2,targetX,t);
    labelY=cubicBezierPt(sourceY,cy1,cy2,targetY,t);
    const tx=cubicBezierPt(sourceX,cx1,cx2,targetX,t+0.01)-labelX;
    const ty=cubicBezierPt(sourceY,cy1,cy2,targetY,t+0.01)-labelY;
    const len=Math.sqrt(tx*tx+ty*ty)||1;
    labelX+=(-ty/len)*14;labelY+=(tx/len)*14;
  } else {
    [edgePath,labelX,labelY]=getBezierPath({sourceX,sourceY,sourcePosition,targetX,targetY,targetPosition,curvature});
    labelX=sourceX+(labelX-sourceX)*1.3;
    labelY=sourceY+(labelY-sourceY)*1.3;
    const dx=targetX-sourceX,dy=targetY-sourceY,len=Math.sqrt(dx*dx+dy*dy)||1;
    labelX+=(-dy/len)*14;labelY+=(dx/len)*14;
  }

  return(
    <>
      <BaseEdge id={id} path={edgePath} markerEnd={markerEnd}
        style={{...style,stroke:arcColor,strokeWidth:width,opacity:0.85}}/>
      {freq>0&&(
        <EdgeLabelRenderer>
          <div style={{position:'absolute',transform:`translate(-50%,-50%) translate(${labelX}px,${labelY}px)`,
            background:'rgba(255,255,255,0.95)',padding:'2px 7px',borderRadius:4,fontSize:11,fontWeight:800,
            border:`1px solid ${C.border}`,pointerEvents:'all',boxShadow:'0 2px 4px rgba(0,0,0,0.1)',color:'#323130'}}>
            {freq.toLocaleString()}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
});

const ProcessNode=React.memo(({data})=>{
  const freq=data.frequency||0;
  const isGrey=data.color==='#94a3b8';
  
  return(
    <div style={{
      background: data.color || '#10b981',
      border: `2px solid ${data.color || '#10b981'}`,
      borderRadius: 12,
      minWidth: 200,
      maxWidth: 280,
      padding: '16px 22px',
      textAlign: 'center',
      boxShadow: '0 8px 24px rgba(0,0,0,0.12)',
      fontFamily: "'Segoe UI', sans-serif",
      display: 'flex',
      flexDirection: 'column',
      gap: 8,
      position: 'relative',
      cursor: 'pointer'
    }}>
      <Handle type="target" id="top-t"    position={Position.Top}    style={{opacity:0}}/>
      <Handle type="source" id="top-s"    position={Position.Top}    style={{opacity:0}}/>
      <Handle type="target" id="bottom-t" position={Position.Bottom} style={{opacity:0}}/>
      <Handle type="source" id="bottom-s" position={Position.Bottom} style={{opacity:0}}/>
      <Handle type="target" id="left-t"   position={Position.Left}   style={{opacity:0}}/>
      <Handle type="source" id="left-s"   position={Position.Left}   style={{opacity:0}}/>
      <Handle type="target" id="right-t"  position={Position.Right}  style={{opacity:0}}/>
      <Handle type="source" id="right-s"  position={Position.Right}  style={{opacity:0}}/>
      <div style={{fontSize:15,fontWeight:700,color:'#fff',lineHeight:1.3,wordBreak:'break-word'}}>{data.label}</div>
      <div style={{fontSize:13,fontWeight:700,color:'#fff',background:'rgba(255,255,255,0.25)',
        padding:'4px 14px',borderRadius:20,display:'inline-block',alignSelf:'center',
        border:'1px solid rgba(255,255,255,0.2)'}}>
        {freq.toLocaleString()} orders
      </div>
    </div>
  );
});

const nodeTypes={processNode:ProcessNode};
const edgeTypes={freqEdge:FreqEdge};

/* ─── P2I HAPPY PATH & LAYOUT ─── */
const P2I_HAPPY_PATH=[
  "Create Purchase Requisition","Create Purchase Order","Goods Receipt (Raw Material)",
  "Create Production Order","Release Production Order","Reserve Component",
  "Goods Issue to WIP","Goods Receipt (Finished Good)","Release from QA","Technically Complete (TECO)"
];
const P2I_HAPPY_IDX=Object.fromEntries(P2I_HAPPY_PATH.map((n,i)=>[n,i]));

const P2I_LEFT_TB=new Set(["Reverse Goods Issue","Record Scrap"]);
const P2I_RIGHT_TB=new Set(["Reverse Goods Receipt","Record Rework"]);
const P2I_ABOVE_LR=new Set(["Reverse Goods Issue","Reverse Goods Receipt"]);
const P2I_BELOW_LR=new Set(["Record Scrap","Record Rework"]);

const classifyP2IEdge=(src,tgt,sPos,tPos,dir)=>{
  const sIdx=P2I_HAPPY_IDX[src],tIdx=P2I_HAPPY_IDX[tgt];
  const sIsHappy=sIdx!==undefined,tIsHappy=tIdx!==undefined;
  if(sIsHappy&&tIsHappy){
    const steps=tIdx-sIdx;
    if(steps===1){
      if(dir==='LR') return{sh:'right-s',th:'left-t',curvature:0.1};
      return{sh:'bottom-s',th:'top-t',curvature:0.1};
    }
    if(steps>1){
      const sweepDist=150+steps*120;
      if(dir==='LR') return{sh:'top-s',th:'top-t',sweepSide:'top',sweepDist};
      return{sh:'right-s',th:'right-t',sweepSide:'right',sweepDist};
    }
    if(steps<0){
      const sweepDist=150+Math.abs(steps)*120;
      if(dir==='LR') return{sh:'bottom-s',th:'bottom-t',sweepSide:'bottom',sweepDist};
      return{sh:'left-s',th:'left-t',sweepSide:'left',sweepDist};
    }
  }
  if(dir==='TB'){
    if(sIsHappy&&P2I_LEFT_TB.has(tgt))  return{sh:'left-s',th:'right-t',curvature:0.5};
    if(P2I_LEFT_TB.has(src)&&tIsHappy)  return{sh:'right-s',th:'left-t',curvature:0.5};
    if(sIsHappy&&P2I_RIGHT_TB.has(tgt)) return{sh:'right-s',th:'left-t',curvature:0.5};
    if(P2I_RIGHT_TB.has(src)&&tIsHappy) return{sh:'left-s',th:'right-t',curvature:0.5};
  } else {
    if(sIsHappy&&P2I_ABOVE_LR.has(tgt)) return{sh:'top-s',th:'bottom-t',curvature:0.5};
    if(P2I_ABOVE_LR.has(src)&&tIsHappy) return{sh:'bottom-s',th:'top-t',curvature:0.5};
    if(sIsHappy&&P2I_BELOW_LR.has(tgt)) return{sh:'bottom-s',th:'top-t',curvature:0.5};
    if(P2I_BELOW_LR.has(src)&&tIsHappy) return{sh:'top-s',th:'bottom-t',curvature:0.5};
  }
  const dx=tPos.x-sPos.x,dy=tPos.y-sPos.y;
  if(dir==='LR'){
    if(dy>0) return{sh:'bottom-s',th:'top-t',curvature:0.4};
    return{sh:'top-s',th:'bottom-t',curvature:0.4};
  }
  if(dx>0) return{sh:'right-s',th:'left-t',curvature:0.4};
  return{sh:'left-s',th:'right-t',curvature:0.4};
};

const buildP2IFlowMap=(bNodes,bEdges,setRfNodes,setRfEdges,dir)=>{
  const mxF=Math.max(1,...(bNodes||[]).map(n=>n.frequency||0));
  const mxE=Math.max(1,...(bEdges||[]).map(e=>e.value||0));
  const nodes=(bNodes||[]).map(n=>{
    const pos=dir==='LR'?(n.position_h||{x:0,y:240}):(n.position_v||{x:400,y:0});
    return{id:n.id,type:'processNode',position:pos,data:{label:n.label,color:n.color,frequency:n.frequency||0,is_main:n.is_main,maxFreq:mxF}};
  });
  const edges=(bEdges||[]).map((e,i)=>{
    const sN=nodes.find(n=>n.id===e.source),tN=nodes.find(n=>n.id===e.target);
    if(!sN||!tN) return null;
    const {sh,th,curvature,sweepSide,sweepDist}=classifyP2IEdge(e.source,e.target,sN.position,tN.position,dir);
    return{
      id:`e${i}`,source:e.source,target:e.target,
      sourceHandle:sh,targetHandle:th,
      type:'freqEdge',
      markerEnd:{type:MarkerType.ArrowClosed,color:'#605E5C', width: 15, height: 15},
      data:{value:e.value,frequency:e.value,maxFreq:mxE,curvature,sweepSide,sweepDist}
    };
  }).filter(Boolean);
  setRfNodes(nodes);setRfEdges(edges);
};

/* ─── TOOLTIP ─── */
const CustomTooltip=({active,payload,nameKey,labelOverride})=>{
  if(!active||!payload?.length||!payload[0]) return null;
  const entry=payload[0].payload||{};
  const name=nameKey?(entry[nameKey]??''):'';
  const val=payload[0].value;
  return(
    <div style={{background:'rgba(255,255,255,.98)',border:`1px solid ${C.border}`,borderRadius:8,padding:'10px 16px',boxShadow:'0 8px 24px rgba(0,0,0,.18)',fontSize:12,color:'#323130',maxWidth:260,zIndex:9999}}>
      {name&&<div style={{fontWeight:700,marginBottom:6,color:C.jkBlue,wordBreak:'break-word',fontSize:13}}>{name}</div>}
      <div style={{color:'#605E5C',display:'flex',justifyContent:'space-between',gap:12}}>
        <span>{labelOverride||'Value:'}</span>
        <strong style={{color:'#000'}}>{val!=null?Number(val).toLocaleString():0}</strong>
      </div>
    </div>
  );
};

/* ─── KPI CARD ─── */
const KpiCard=React.memo(({label,value,color,suffix='',highlighted,onClick,tooltip})=>{
  const [hover,setHover]=useState(false);
  const bColor=hover?'rgba(106,51,130,0.5)':(highlighted?C.selectedBorder:'transparent');
  const bWidth='1.5px';
  return(
    <div onClick={onClick} onMouseEnter={()=>setHover(true)} onMouseLeave={()=>setHover(false)} style={{
      background:highlighted?C.selected:C.card,borderRadius:6,padding:'10px 14px',
      borderTop:`${bWidth} solid ${bColor}`,borderRight:`${bWidth} solid ${bColor}`,
      borderBottom:`${bWidth} solid ${bColor}`,borderLeft:'4px solid #6a3382',
      boxShadow:hover?'0 6px 16px rgba(106,51,130,.15)':'0 2px 6px rgba(0,0,0,.05)',
      transition:'all .2s',cursor:onClick?'pointer':'default',minWidth:0,position:'relative',
      display:'flex',flexDirection:'column',alignItems:'center',textAlign:'center',
      transform:hover?'translateY(-3px)':'none',boxSizing:'border-box',zIndex:hover?50:1
    }}>
      <div style={{fontSize:10,fontWeight:600,color:'#6a3382',textTransform:'uppercase',letterSpacing:.5,marginBottom:2}}>{label}</div>
      <div style={{fontSize:20,fontWeight:600,color:'#000000',lineHeight:1}}>
        {value!=null?`${Number(value).toLocaleString()}${suffix}`:'—'}
      </div>
      {hover&&tooltip&&(
        <div style={{position:'absolute',top:'100%',left:0,marginTop:4,background:'#fff',border:`1px solid ${C.border}`,borderRadius:4,padding:'6px 10px',boxShadow:'0 4px 12px rgba(0,0,0,.15)',fontSize:11,color:'#323130',zIndex:100,whiteSpace:'nowrap',textAlign:'left'}}>
          {tooltip}
        </div>
      )}
    </div>
  );
});

const ConfKpiCard=React.memo(({label,value,sub,tooltip,onClick,highlighted})=>{
  const [hover,setHover]=useState(false);
  const bColor=hover?'rgba(106,51,130,0.5)':(highlighted?C.selectedBorder:'transparent');
  const bWidth='1.5px';
  return(
    <div onClick={onClick} onMouseEnter={()=>setHover(true)} onMouseLeave={()=>setHover(false)} style={{
      background:highlighted?C.selected:C.card,borderRadius:6,padding:'10px 14px',
      borderTop:`${bWidth} solid ${bColor}`,borderRight:`${bWidth} solid ${bColor}`,
      borderBottom:`${bWidth} solid ${bColor}`,borderLeft:'4px solid #6a3382',
      boxShadow:hover?'0 6px 16px rgba(106,51,130,.15)':'0 2px 6px rgba(0,0,0,.05)',
      transition:'all .2s',cursor:onClick?'pointer':'default',minWidth:0,position:'relative',
      display:'flex',flexDirection:'column',alignItems:'center',textAlign:'center',
      transform:hover?'translateY(-3px)':'none',boxSizing:'border-box',zIndex:hover?50:1
    }}>
      <div style={{fontSize:10,fontWeight:600,color:'#6a3382',textTransform:'uppercase',letterSpacing:.5,marginBottom:2}}>{label}</div>
      <div style={{fontSize:20,fontWeight:600,color:'#000000',lineHeight:1}}>
        {value!=null?Number(value).toLocaleString():'—'}
      </div>
      {sub&&<div style={{fontSize:10,color:C.slate,marginTop:3}}>{sub}</div>}
      {hover&&tooltip&&(
        <div style={{position:'absolute',top:'100%',left:0,marginTop:4,background:'#fff',border:`1px solid ${C.border}`,borderRadius:4,padding:'6px 10px',boxShadow:'0 4px 12px rgba(0,0,0,.15)',fontSize:11,color:'#323130',zIndex:100,whiteSpace:'nowrap',textAlign:'left'}}>
          {tooltip}
        </div>
      )}
    </div>
  );
});

/* ─── SEARCHABLE SELECT ─── */
const SearchableSelect=({label,value,options,onChange})=>{
  const [isOpen,setIsOpen]=useState(false);
  const [search,setSearch]=useState('');
  const ref=useRef(null);
  useEffect(()=>{
    const click=(e)=>{if(ref.current&&!ref.current.contains(e.target)) setIsOpen(false);};
    document.addEventListener('mousedown',click); return ()=>document.removeEventListener('mousedown',click);
  },[]);
  const filtered=['ALL',...options].filter(o=>String(o).toLowerCase().includes(search.toLowerCase()));
  return(
    <div style={{display:'flex',flexDirection:'column',gap:3,flex:1,minWidth:'150px',position:'relative'}} ref={ref}>
      <label style={{fontSize:10,fontWeight:700,color:'#323130',textTransform:'uppercase',letterSpacing:.4}}>{label}</label>
      <div onClick={()=>setIsOpen(!isOpen)} style={{fontSize:12,padding:'5px 10px',borderRadius:4,width:'100%',border:value&&value!=='ALL'?`1.5px solid ${C.blue700}`:`1px solid ${C.border}`,background:value&&value!=='ALL'?'#EFF6FF':C.card,color:'#323130',cursor:'pointer',fontWeight:value&&value!=='ALL'?700:'normal',display:'flex',justifyContent:'space-between',alignItems:'center'}}>
        <span style={{whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{value||'ALL'}</span>
        <span style={{fontSize:10,opacity:0.6}}>▼</span>
      </div>
      {isOpen&&(
        <div style={{position:'absolute',top:'100%',left:0,right:0,zIndex:1000,background:'#fff',border:`1px solid ${C.border}`,borderRadius:4,boxShadow:'0 8px 24px rgba(0,0,0,0.15)',maxHeight:220,overflowY:'auto',marginTop:4}}>
          <input type="text" placeholder="Search..." value={search} onChange={e=>setSearch(e.target.value)} onClick={e=>e.stopPropagation()} style={{width:'100%',padding:'8px',border:'none',borderBottom:`1px solid ${C.border}`,fontSize:12,outline:'none'}} autoFocus/>
          {filtered.map((o,i)=>(
            <div key={i} onClick={()=>{onChange(o);setIsOpen(false);setSearch('');}} style={{padding:'8px 10px',fontSize:12,cursor:'pointer',background:value===o?'#EFF6FF':'#fff',color:value===o?C.blue700:'#323130',borderLeft:value===o?`3px solid ${C.blue700}`:'3px solid transparent'}} onMouseEnter={e=>{if(value!==o) e.currentTarget.style.background='#F3F2F1';}} onMouseLeave={e=>{if(value!==o) e.currentTarget.style.background='#fff';}}>{o}</div>
          ))}
        </div>
      )}
    </div>
  );
};

/* ─── ORDER TIMELINE ─── */
const OrderTimeline=({orderId,username})=>{
  const [events,setEvents]=useState([]);
  const [loading,setLoading]=useState(false);
  useEffect(()=>{
    if(!orderId||orderId==='ALL') return;
    setLoading(true);
    fetch(`${API}/p2i/order_timeline?username=${username}&order_id=${orderId}`)
      .then(r=>r.json()).then(d=>setEvents(d)).finally(()=>setLoading(false));
  },[orderId,username]);
  if(!orderId||orderId==='ALL') return <div style={{height:140,display:'flex',alignItems:'center',justifyContent:'center',color:C.slate,fontSize:12,border:`1px dashed ${C.border}`,borderRadius:6,background:'#f8fafc'}}>Select an Order ID to view timeline</div>;
  if(loading) return <div style={{height:140,display:'flex',alignItems:'center',justifyContent:'center'}}><div style={{width:20,height:20,borderRadius:'50%',border:`2px solid ${C.blue700}`,borderTopColor:'transparent',animation:'spin 0.8s linear infinite'}}/></div>;
  return(
    <div style={{display:'flex',flexDirection:'column',gap:0,maxHeight:400,overflowY:'auto',padding:'4px 0'}}>
      {events.map((ev,i)=>(
        <div key={i} style={{display:'flex',gap:12,padding:'10px 0',position:'relative'}}>
          <div style={{width:12,display:'flex',flexDirection:'column',alignItems:'center',flexShrink:0}}>
            <div style={{width:10,height:10,borderRadius:'50%',background:ev.is_dev?'#ef4444':(ev.is_bridge?'#8b5cf6':C.jkBlue),zIndex:2,marginTop:4}}/>
            {i<events.length-1&&<div style={{width:2,flex:1,background:'#E1DFDD',margin:'4px 0'}}/>}
          </div>
          <div style={{flex:1}}>
            <div style={{fontSize:13,fontWeight:700,color:'#1e293b'}}>{ev.activity}</div>
            <div style={{fontSize:11,color:C.slate}}>{new Date(ev.timestamp).toLocaleString()}</div>
          </div>
        </div>
      ))}
    </div>
  );
};

/* ─── FILTER SELECT ─── */
const FilterSelect=({label,value,options,onChange})=>(
  <div style={{display:'flex',flexDirection:'column',gap:3,flex:1,minWidth:'140px'}}>
    <label style={{fontSize:10,fontWeight:700,color:'#323130',textTransform:'uppercase',letterSpacing:.4}}>{label}</label>
    <select value={value} onChange={e=>onChange(e.target.value)} style={{fontSize:12,padding:'5px 8px',borderRadius:4,width:'100%',border:value&&value!=='ALL'?`1.5px solid ${C.blue700}`:`1px solid ${C.border}`,background:value&&value!=='ALL'?'#EFF6FF':C.card,color:'#323130',outline:'none',cursor:'pointer',fontWeight:value&&value!=='ALL'?700:'normal'}}>
      {(Array.isArray(options)?options:['ALL']).map(o=>(<option key={o} value={o}>{o}</option>))}
    </select>
  </div>
);

/* ─── CHART CARD ─── */
const ChartCard=React.memo(({title,subtitle,children,highlighted,onClear,style={},loading=false})=>(
  <div style={{background:C.card,borderRadius:8,padding:'12px 14px',border:highlighted?`1.5px solid ${C.selectedBorder}`:`1px solid ${C.border}`,boxShadow:'0 2px 8px rgba(0,0,0,.05)',transition:'all .2s',display:'flex',flexDirection:'column',...style}}>
    <div style={{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:8}}>
      <div>
        <div style={{fontSize:13,fontWeight:700,color:'#323130'}}>{title}</div>
        {subtitle&&<div style={{fontSize:10,color:'#8A8886',marginTop:2}}>{subtitle}</div>}
      </div>
      {highlighted&&onClear&&(
        <button onClick={onClear} style={{fontSize:11,color:'#fff',background:C.blue700,border:'none',borderRadius:4,padding:'3px 9px',cursor:'pointer',fontWeight:600,flexShrink:0}}>Clear</button>
      )}
    </div>
    <div className={!loading ? "fade-in" : ""} style={{flex:1,minHeight:0,position:'relative'}}>
      {loading&&(
        <div style={{position:'absolute',inset:0,zIndex:10,background:'rgba(255,255,255,0.85)',backdropFilter:'blur(2px)',display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',gap:8}}>
          <div style={{width:20,height:20,borderRadius:'50%',border:`2px solid ${C.blue700}`,borderTopColor:'transparent',animation:'spin 0.8s linear infinite'}}/>
          <div style={{fontSize:11,fontWeight:600,color:C.blue700}}>Analysing...</div>
        </div>
      )}
      {children}
    </div>
  </div>
));

const Empty=()=>(<div style={{height:90,display:'flex',alignItems:'center',justifyContent:'center',color:C.slate,fontSize:12}}>No data available</div>);

const EmptyState=({condition,message,children,action})=>{
  if(!condition) return children;
  return(
    <div style={{display:'flex',alignItems:'center',justifyContent:'center',height:'220px',background:'#F8FAFC',borderRadius:'6px',border:'1px dashed #CBD5E1',color:'#64748b',fontSize:'13px',padding:'20px',textAlign:'center',flexDirection:'column',gap:'12px'}}>
      <div style={{fontSize:'28px',opacity:0.8}}>📉</div>
      <div style={{fontWeight:600,maxWidth:'250px'}}>{message}</div>
      {action}
    </div>
  );
};

/* ─── CHARTS ─── */
const LeadTimeMtartChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="No lead time data available"/>;
  const af=crossFilter?.type==='mtart'?crossFilter.value:null;
  return(
    <div style={{width:'100%',height:220}}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{left:20,right:20,top:4,bottom:4}}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false}/>
          <XAxis type="number" tick={{fontSize:11,fill:'#605E5C'}} label={{value:'Days',position:'insideRight',offset:10,style:{fontSize:10,fill:'#8A8886'}}}/>
          <YAxis dataKey="name" type="category" width={110} tick={{fontSize:11,fill:'#605E5C'}} interval={0}/>
          <Tooltip content={<CustomTooltip nameKey="name" labelOverride="Avg Days:"/>}/>
          <Bar dataKey="value" radius={[0,4,4,0]} barSize={22}
            onClick={e=>e?.name&&onSelect('mtart',e.name===af?null:e.name)}>
            {data.map((entry,i)=>(
              <Cell key={i} cursor="pointer" fill={af===entry.name?C.orange:C.teal} opacity={af&&af!==entry.name?0.3:1}/>
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

const DeviationsChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="No deviation data available"/>;
  const af=crossFilter?.type==='deviation'?crossFilter.value:null;
  return(
    <div style={{width:'100%',height:220}}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{left:8,right:8,top:8,bottom:50}}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false}/>
          <XAxis dataKey="name" tick={{fontSize:10,fill:'#605E5C'}} angle={-35} textAnchor="end" interval={0}/>
          <YAxis tick={{fontSize:10,fill:'#605E5C'}} width={44}/>
          <Tooltip content={<CustomTooltip nameKey="name" labelOverride="Count:"/>}/>
          <Bar dataKey="value" radius={[4,4,0,0]} barSize={32}
            onClick={e=>e?.name&&onSelect('deviation',e.name===af?null:e.name)}>
            {data.map((entry,i)=>(
              <Cell key={i} cursor="pointer" fill={af===entry.name?C.orange:(entry.name.includes('Over')||entry.name.includes('Delay')?C.orange:C.red)} opacity={af&&af!==entry.name?0.3:1}/>
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

const MonthlyTrendChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="No trend data available"/>;
  const af=crossFilter?.type==='month'?crossFilter.value:null;
  return(
    <div style={{width:'100%',height:220}}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{left:8,right:8,top:10,bottom:40}}
          onClick={e=>{
            const month=e?.activeLabel;
            if(month) onSelect('month',month===af?null:month);
          }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9"/>
          <XAxis dataKey="month" tick={{fontSize:10,fill:'#605E5C',fontWeight:af?'bold':'normal'}} angle={-45} textAnchor="end" interval={0}/>
          <YAxis tick={{fontSize:10,fill:'#605E5C'}} width={40}/>
          <Tooltip content={<CustomTooltip nameKey="month" labelOverride="Orders:"/>}/>
          <Line type="monotone" dataKey="count" stroke={af?C.orange:"#0078D4"} strokeWidth={3} dot={{r:4,fill:af?C.orange:"#0078D4"}} activeDot={{r:7,fill:C.orange,stroke:'#fff',strokeWidth:2}}/>
          {af&&<ReferenceLine x={af} stroke={C.orange} strokeDasharray="3 3"/>}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
});

const PlantBarChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="No plant data available"/>;
  const af=crossFilter?.type==='plant'?crossFilter.value:null;
  return(
    <div style={{width:'100%',height:220,overflowY:'auto'}}>
      <div style={{height:Math.max(220,data.length*30)}}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{left:10,right:20,top:4,bottom:4}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false}/>
            <XAxis type="number" hide/>
            <YAxis dataKey="name" type="category" width={80} tick={{fontSize:10,fill:'#605E5C'}} interval={0}/>
            <Tooltip content={<CustomTooltip nameKey="name" labelOverride="Orders:"/>}/>
            <Bar dataKey="value" radius={[0,3,3,0]} barSize={20}
              onClick={e=>e?.name&&onSelect('plant',e.name===af?null:e.name)}>
              {data.map((entry,i)=>(
                <Cell key={i} cursor="pointer" fill={af===entry.name?C.orange:C.purple} opacity={af&&af!==entry.name?0.25:1}/>
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

/* ─── FAQ ITEM ─── */
const FaqItem=({q,a,bullets,accentColor})=>{
  const [open,setOpen]=useState(false);
  const accent=accentColor||'#0078D4';
  return(
    <div style={{borderBottom:'1px solid #E2E8F0'}}>
      <button onClick={()=>setOpen(o=>!o)} style={{width:'100%',display:'flex',justifyContent:'space-between',alignItems:'center',padding:'14px 0',background:'none',border:'none',cursor:'pointer',textAlign:'left',gap:12}}>
        <span style={{fontSize:13.5,fontWeight:600,color:'#1e293b',lineHeight:1.4}}>{q}</span>
        <span style={{fontSize:18,color:accent,flexShrink:0,fontWeight:700,transform:open?'rotate(45deg)':'none',transition:'transform 0.2s',display:'inline-block',width:20,textAlign:'center'}}>+</span>
      </button>
      {open&&(
        <div style={{paddingBottom:16,fontSize:13,color:'#475569',lineHeight:1.75}}>
          {a&&<p style={{margin:'0 0 8px'}}>{a}</p>}
          {bullets&&bullets.length>0&&(
            <ul style={{margin:0,paddingLeft:20,display:'flex',flexDirection:'column',gap:5}}>
              {bullets.map((b,i)=>(
                <li key={i} style={{color:'#334155',lineHeight:1.6}}>
                  {typeof b==='object'&&b.bold?<><strong style={{color:'#1e293b'}}>{b.bold}</strong>{b.rest}</>:b}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
};

const P2I_FAQS=[
  {
    q:'What is the Plan-to-Inventory (P2I) process?',
    a:'P2I covers the full manufacturing lifecycle — from production order creation through final goods receipt. It ensures materials are available, operations are sequenced, and quality is verified before stock enters inventory. Key stages include:',
    bullets:[
      {bold:'Production Order Creation:',rest:' SAP order generated from planning (MRP/manual)'},
      {bold:'Component Reservation:',rest:' Materials reserved for production (RESB)'},
      {bold:'Goods Issue to WIP:',rest:' Components issued to production order (BWART 261)'},
      {bold:'Operation Confirmations:',rest:' Routing steps confirmed (AFRU)'},
      {bold:'Goods Receipt (FG):',rest:' Finished goods posted to stock (BWART 101)'},
      {bold:'Technical Completion (TECO):',rest:' Order closed after all activities complete'},
    ],
  },
  {
    q:'What is Process Mining, and how does it apply to P2I?',
    a:'Process Mining analyzes real event log data from SAP to reconstruct how production actually runs — not just how it was planned. In P2I it reveals:',
    bullets:[
      'Actual vs planned production sequences and timing',
      'Bottlenecks in operations causing schedule delays',
      'Scrap and rework hotspots by operation or material type',
      'Over-production events where yield exceeds planned quantity',
    ],
  },
  {
    q:'Why use Process Mining specifically for P2I?',
    a:'Production processes are complex, multi-step, and cross-functional. Process Mining delivers:',
    bullets:[
      {bold:'True visibility:',rest:' See actual execution paths, not just planned routes'},
      {bold:'Deviation detection:',rest:' Identify scrap, rework, and reversals automatically'},
      {bold:'Lead time analysis:',rest:' Measure actual vs standard production times'},
      {bold:'Bottleneck prioritization:',rest:' Focus improvement efforts on high-impact steps'},
      {bold:'Compliance tracking:',rest:' Confirm production follows standard SAP workflows'},
    ],
  },
  {
    q:'What are the most common issues Process Mining uncovers in P2I?',
    a:'Typical findings include:',
    bullets:[
      'Material availability delays causing GI to lag reservations',
      'High scrap or rework rates in specific operations',
      'Over-production — finished goods exceeding order quantity',
      'Long gaps between order creation and release',
      'Missing TECO closure on completed orders',
    ],
  },
  {
    q:'What key metrics (KPIs) does Process Mining track in P2I?',
    a:'Common KPIs include:',
    bullets:[
      {bold:'Production Lead Time:',rest:' Days from order creation to FG goods receipt'},
      {bold:'Scrap Rate:',rest:' % of orders with recorded scrap confirmations'},
      {bold:'Rework Rate:',rest:' % of orders requiring re-processing'},
      {bold:'TECO Completion Rate:',rest:' % of orders technically completed'},
      {bold:'Material Delay Rate:',rest:' Orders where GI was later than reservation date'},
      {bold:'Over-Production Rate:',rest:' Orders where actual yield exceeded planned quantity'},
    ],
  },
  {
    q:'What SAP tables are required for P2I process mining?',
    a:'The core tables needed are:',
    bullets:[
      {bold:'AFKO:',rest:' Order header — dates, status, basic data'},
      {bold:'AFPO:',rest:' Order item — material and planned quantity'},
      {bold:'RESB:',rest:' Component reservations — material availability'},
      {bold:'MKPF/MSEG:',rest:' Material documents — goods movements (GI, GR, reversals)'},
      {bold:'AFRU:',rest:' Order confirmations — operation completions, scrap, rework'},
      {bold:'MARA:',rest:' Material master — material type for grouping'},
      {bold:'EBAN/EKKO/EKPO/EKBE:',rest:' (Optional) P2P Bridge tables to link procurement events to production orders.'},
    ],
  },
];

/* ─── P2I INTRO SCREEN ─── */
const P2IIntroScreen=({onGoTableBuild,onGoCsvUpload,currentUser})=>{
  const [introStep,setIntroStep]=useState('overview');

  const steps=[
    'Production Order created and released in SAP (AFKO/AFPO)',
    'Optional: Upstream Purchase Requisitions and Orders linked via P2P Bridge (EBAN/EKKO/EKPO)',
    'Component materials reserved and issued to WIP (RESB → BWART 261)',
    'Operations confirmed step-by-step on the shop floor (AFRU)',
    'Finished Good posted to inventory via Goods Receipt (BWART 101)',
    'Quality checks completed — order Technically Complete (TECO)',
  ];
  const kpis=[
    'Production lead time (create to FG receipt)',
    'Scrap and rework rates by material type',
    'Material availability delay analysis',
    'Over-production detection',
    'Operation bottleneck identification',
    'TECO completion rate and WIP ageing',
  ];

  if(introStep==='overview') return(
    <div style={{flex:1,display:'flex',flexDirection:'column',alignItems:'center',padding:'36px 24px 48px',overflowY:'auto'}}>
      <div style={{maxWidth:880,width:'100%',display:'flex',flexDirection:'column',gap:24}}>
        <div style={{borderBottom:'2px solid #E2E8F0',paddingBottom:20}}>
          <div style={{fontSize:11,fontWeight:700,color:'#038387',textTransform:'uppercase',letterSpacing:1,marginBottom:6}}>Process Overview</div>
          <h1 style={{margin:'0 0 8px',fontSize:24,fontWeight:700,color:'#1e293b'}}>Plan-to-Inventory (P2I)</h1>
          <p style={{margin:0,fontSize:13.5,color:'#475569',lineHeight:1.7,maxWidth:720}}>
            The Plan-to-Inventory process spans the full manufacturing lifecycle — from production order creation
            through material issuance, shop floor confirmations, and final goods receipt. Process mining on P2I
            data uncovers production delays, scrap hotspots, over-production events, and deviation patterns
            across your manufacturing operations.
          </p>
        </div>

        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:20}}>
          <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 22px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:14}}>Process Steps</div>
            <ol style={{margin:0,padding:'0 0 0 18px',display:'flex',flexDirection:'column',gap:10}}>
              {steps.map((s,i)=>(<li key={i} style={{fontSize:13,color:'#334155',lineHeight:1.5}}><span style={{fontWeight:600,color:'#038387'}}>Step {i+1}.</span>{' '}{s}</li>))}
            </ol>
          </div>
          <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 22px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:14}}>Key Metrics Analysed</div>
            <ul style={{margin:0,padding:'0 0 0 18px',display:'flex',flexDirection:'column',gap:10}}>
              {kpis.map((k,i)=>(<li key={i} style={{fontSize:13,color:'#334155',lineHeight:1.5}}>{k}</li>))}
            </ul>
          </div>
        </div>

        <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 24px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:4}}>Frequently Asked Questions</div>
          <p style={{fontSize:12,color:'#94a3b8',margin:'0 0 16px'}}>Click a question to expand the answer</p>
          <div>{P2I_FAQS.map((faq,i)=><FaqItem key={i} q={faq.q} a={faq.a} bullets={faq.bullets} accentColor="#038387"/>)}</div>
        </div>

        <div style={{display:'flex',justifyContent:'flex-end',paddingTop:4}}>
          <button
            onClick={()=>setIntroStep('choose')}
            style={{background:'#038387',color:'#fff',border:'none',padding:'12px 32px',borderRadius:8,fontSize:14,fontWeight:700,cursor:'pointer',display:'flex',alignItems:'center',gap:8,boxShadow:'0 4px 12px rgba(3,131,135,0.25)',transition:'all 0.2s'}}
            onMouseOver={e=>{e.currentTarget.style.background='#026769';}}
            onMouseOut={e=>{e.currentTarget.style.background='#038387';}}>
            Continue <span style={{fontSize:16}}>→</span>
          </button>
        </div>
      </div>
    </div>
  );

  return(
    <div style={{flex:1,display:'flex',flexDirection:'column',alignItems:'center',padding:'36px 24px 48px',overflowY:'auto'}}>
      <div style={{maxWidth:880,width:'100%',display:'flex',flexDirection:'column',gap:24}}>
        <div>
          <button onClick={()=>setIntroStep('overview')} style={{background:'none',border:'1px solid #E2E8F0',padding:'6px 14px',borderRadius:6,fontSize:12,cursor:'pointer',color:'#64748b',fontWeight:600}}>← Back to Overview</button>
        </div>
        <div style={{textAlign:'center'}}>
          <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:8}}>Get Started</div>
          <h2 style={{margin:'0 0 6px',fontSize:20,fontWeight:700,color:'#1e293b'}}>Choose how to load your data</h2>
          <p style={{margin:0,fontSize:13,color:'#64748b'}}>Select the method that matches your data format</p>
        </div>

        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:20}}>
          <div style={{background:'#fff',border:'2px solid #E2E8F0',borderRadius:12,padding:'28px 24px',display:'flex',flexDirection:'column',alignItems:'center',gap:14,textAlign:'center',boxShadow:'0 2px 8px rgba(0,0,0,0.05)',transition:'all 0.2s',cursor:'pointer'}}
            onClick={onGoTableBuild}
            onMouseEnter={e=>{e.currentTarget.style.borderColor='#038387';e.currentTarget.style.boxShadow='0 4px 16px rgba(3,131,135,0.15)';}}
            onMouseLeave={e=>{e.currentTarget.style.borderColor='#E2E8F0';e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.05)';}}>
            <div style={{width:60,height:60,borderRadius:14,background:'#F0FDFA',display:'flex',alignItems:'center',justifyContent:'center',fontSize:28}}>🔨</div>
            <div>
              <div style={{fontSize:16,fontWeight:700,color:'#1e293b',marginBottom:8}}>Build Event Log</div>
              <div style={{fontSize:13,color:'#64748b',lineHeight:1.65}}>Upload raw SAP tables (AFKO, AFPO, RESB, MKPF, MSEG, AFRU, MARA) and optional Bridge tables (EBAN, EKKO, EKPO, EKBE) to build the full production timeline.</div>
            </div>
            <button onClick={e=>{e.stopPropagation();onGoTableBuild();}} style={{background:'#038387',color:'#fff',border:'none',padding:'11px 28px',borderRadius:7,fontSize:13,fontWeight:700,cursor:'pointer',width:'100%',marginTop:'auto'}}
              onMouseOver={e=>e.currentTarget.style.background='#026769'} onMouseOut={e=>e.currentTarget.style.background='#038387'}>
              Build Event Log
            </button>
          </div>

          <div style={{background:'#fff',border:'2px solid #E2E8F0',borderRadius:12,padding:'28px 24px',display:'flex',flexDirection:'column',alignItems:'center',gap:14,textAlign:'center',boxShadow:'0 2px 8px rgba(0,0,0,0.05)',transition:'all 0.2s',cursor:'pointer'}}
            onClick={onGoCsvUpload}
            onMouseEnter={e=>{e.currentTarget.style.borderColor='#0078D4';e.currentTarget.style.boxShadow='0 4px 16px rgba(0,120,212,0.15)';}}
            onMouseLeave={e=>{e.currentTarget.style.borderColor='#E2E8F0';e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.05)';}}>
            <div style={{width:60,height:60,borderRadius:14,background:'#EFF6FF',display:'flex',alignItems:'center',justifyContent:'center',fontSize:28}}>📂</div>
            <div>
              <div style={{fontSize:16,fontWeight:700,color:'#1e293b',marginBottom:8}}>Upload Pre-built CSV</div>
              <div style={{fontSize:13,color:'#64748b',lineHeight:1.65}}>Already have a formatted event log? Upload your pre-built CSV file directly to launch the dashboard.</div>
            </div>
            <button onClick={e=>{e.stopPropagation();onGoCsvUpload();}} style={{background:'#0078D4',color:'#fff',border:'none',padding:'11px 28px',borderRadius:7,fontSize:13,fontWeight:700,cursor:'pointer',width:'100%',marginTop:'auto'}}
              onMouseOver={e=>e.currentTarget.style.background='#005A9E'} onMouseOut={e=>e.currentTarget.style.background='#0078D4'}>
              Upload Pre-built CSV
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

/* ─── TABLE UPLOAD SCREEN ─── */
const TableUploadScreen=({onBuilt,onBack,onLoadingChange,currentUser,myFiles,fetchingFiles,handleLoadOldFile})=>{
  const tables=[
    {name:'AFKO',desc:'Order Header Data',isMandatory:true,required:[
      {col:'AUFNR',note:'Order number — join key'},
      {col:'GSTRP',note:'Scheduled start date → Create Production Order'},
      {col:'FTRMI',note:'Actual release date → Release Production Order'},
      {col:'GLTRI',note:'Actual finish date → TECO'},
    ]},
    {name:'AFPO',desc:'Order Item Data',isMandatory:true,required:[
      {col:'AUFNR',note:'Order number — join key'},
      {col:'PSMNG',note:'Planned order quantity — over-production check'},
      {col:'MATNR',note:'Material number — join to MARA'},
      {col:'DWERK',note:'Plant (WERKS) — used for plant filtering'},
    ]},
    {name:'RESB',desc:'Component Reservations',required:[
      {col:'AUFNR',note:'Order number — join key'},
      {col:'BDTER',note:'Requirement date → Reserve Component'},
      {col:'XLOEK',note:'Deletion indicator'},
    ]},
    {name:'MKPF',desc:'Material Document Header',required:[
      {col:'MBLNR',note:'Material document number — join key'},
      {col:'BUDAT',note:'Posting date — used for GI/GR dates'},
    ]},
    {name:'MSEG',desc:'Material Document Segment',required:[
      {col:'MBLNR',note:'Material document — join key'},
      {col:'AUFNR',note:'Order number — join key'},
      {col:'BWART',note:'Movement type (261=GI, 101=GR, 262=rev GI, 102=rev GR, 321=QA)'},
      {col:'MENGE',note:'Quantity — over-production calculation'},
    ]},
    {name:'AFRU',desc:'Order Confirmations',required:[
      {col:'AUFNR',note:'Order number — join key'},
      {col:'VORNR',note:'Operation number → Confirm Operation'},
      {col:'IEDD',note:'Actual finish date/time'},
      {col:'XMZMN',note:'Scrap quantity > 0 → Record Scrap (Deviation)'},
      {col:'RMNGA',note:'Rework quantity > 0 → Record Rework (Deviation)'},
    ]},
    {name:'MARA',desc:'Material Master',required:[
      {col:'MATNR',note:'Material number — join key'},
      {col:'MTART',note:'Material type — filter & grouping'},
    ]},
    {name:'EBAN',desc:'Purchase Requisitions (P2P Bridge)',required:[
      {col:'BANFN',note:'PR number'},
      {col:'BADAT',note:'PR creation date'},
    ]},
    {name:'EKKO',desc:'Purchase Orders Header (P2P Bridge)',required:[
      {col:'EBELN',note:'PO number'},
      {col:'AEDAT',note:'PO creation date'},
    ]},
    {name:'EKPO',desc:'Purchase Orders Item (P2P Bridge)',required:[
      {col:'EBELN',note:'PO number'},
      {col:'EBELP',note:'PO Item number — for precise linking'},
      {col:'BANFN',note:'PR number'},
      {col:'AUFNR',note:'Production Order number — CRITICAL bridge key'},
    ]},
    {name:'EKBE',desc:'Purchasing History (P2P Bridge)',required:[
      {col:'EBELN',note:'PO number'},
      {col:'EBELP',note:'PO Item number'},
      {col:'BUDAT',note:'GR/IR date'},
      {col:'BEWTP',note:'E = Goods Receipt'},
    ]},
  ];

  const [tableStatus,setTableStatus]=useState(Object.fromEntries(tables.map(t=>[t.name,'idle'])));
  const [tableMsg,setTableMsg]=useState(Object.fromEntries(tables.map(t=>[t.name,''])));
  const [building,setBuilding]=useState(false);
  const [buildMsg,setBuildMsg]=useState('');
  const [colMapping,setColMapping]=useState(null);
  const [selectedFiles,setSelectedFiles]=useState({});
  const [appliedMappings,setAppliedMappings]=useState({});
  const fileRefs=useRef(Object.fromEntries(tables.map(t=>[t.name,React.createRef()])));

  const allDone=tables.filter(t=>t.isMandatory).every(t=>tableStatus[t.name]==='done');
  const anyUploading=tables.some(t=>tableStatus[t.name]==='uploading')||building;

  const performUpload=async(tableName,file,mapping)=>{
    setTableStatus(p=>({...p,[tableName]:'uploading'}));
    setTableMsg(p=>({...p,[tableName]:''}));
    const form=new FormData();
    form.append('file',file);form.append('table_name',tableName);form.append('username',currentUser||'Unknown');
    form.append('column_mapping',JSON.stringify(mapping));
    try{
      const r=await fetch(`${API}/p2i/transform/upload_table`,{method:'POST',body:form});
      const d=await r.json();
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      setTableStatus(p=>({...p,[tableName]:'done'}));
      setTableMsg(p=>({...p,[tableName]:`${Number(d.rows).toLocaleString()} rows`}));
      setColMapping(null);
    }catch(e){
      setTableStatus(p=>({...p,[tableName]:'error'}));
      setTableMsg(p=>({...p,[tableName]:e.message}));
    }
  };

  const uploadTable=async(tableName,file)=>{
    if(!file) return;
    if(!file.name.toLowerCase().endsWith('.csv')){
      setTableStatus(p=>({...p,[tableName]:'error'}));
      setTableMsg(p=>({...p,[tableName]:'Only .csv accepted.'}));
      return;
    }
    setSelectedFiles(p=>({...p,[tableName]:file}));
    performUpload(tableName,file,{});
  };

  const handleMapColumns=async(tableName)=>{
    const file=selectedFiles[tableName];
    if(!file) return;
    const reader=new FileReader();
    reader.onload=(e)=>{
      const firstLine=e.target.result.split('\n')[0];
      // Robust split: try comma then semicolon
      let uploadedCols = firstLine.split(',').map(c=>c.trim().replace(/^"|"$/g,''));
      if(uploadedCols.length<2) uploadedCols = firstLine.split(';').map(c=>c.trim().replace(/^"|"$/g,''));
      
      const tDef=tables.find(t=>t.name===tableName);
      setColMapping({tableName,file,tableDef:tDef,uploadedCols,mapping:{}});
    };
    reader.readAsText(file.slice(0,10000));
  };

  const handleBuild=async()=>{
    setBuilding(true);setBuildMsg('');
    onLoadingChange&&onLoadingChange(true,20,'Processing Data...');
    let prog=20;
    const ticker=setInterval(()=>{prog=Math.min(prog+Math.random()*14,88);onLoadingChange&&onLoadingChange(true,prog,'Analysing Data...');},400);
    try{
      const r=await fetch(`${API}/p2i/transform/build?username=${encodeURIComponent(currentUser||'Unknown')}`,{method:'POST'});
      const d=await r.json();
      clearInterval(ticker);
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      
      onLoadingChange&&onLoadingChange(true,100,'Dashboard Created');
      setTimeout(()=>{
        setBuilding(false);
        setBuildMsg(`✓ Success: ${Number(d.rows).toLocaleString()} rows processed`);
        // Force refresh by notifying parent
        onBuilt&&onBuilt(null,'table');
      },800);
    }catch(e){
      clearInterval(ticker);
      onLoadingChange&&onLoadingChange(false,0,'');
      setBuilding(false);
      onBuilt&&onBuilt(e.message,'table');
    }
  };

  const handleClearAll=async()=>{
    if(!window.confirm('Are you sure you want to clear all uploaded tables?')) return;
    try{
      const loaded=Object.keys(tableStatus).filter(t=>tableStatus[t]==='done'||tableStatus[t]==='error');
      for(const tName of loaded){
        await fetch(`${API}/p2i/transform/clear_table?table_name=${tName}&username=${encodeURIComponent(currentUser||'Unknown')}`,{method:'DELETE'}).catch(()=>{});
      }
      setTableStatus(Object.fromEntries(tables.map(t=>[t.name,'idle'])));
      setTableMsg(Object.fromEntries(tables.map(t=>[t.name,''])));
      setSelectedFiles({});setAppliedMappings({});setBuildMsg('All tables cleared.');
    }catch(e){setBuildMsg('Failed to clear some tables.');}
  };

  const si=(s)=>{
    if(s==='done')      return{icon:'✓',color:'#107C10',bg:'#F0FAF0',border:'#107C10'};
    if(s==='error')     return{icon:'✕',color:'#D13438',bg:'#FDE7E9',border:'#D13438'};
    if(s==='uploading') return{icon:'…',color:'#0078D4',bg:'#EFF6FF',border:'#0078D4'};
    return                    {icon:'↑',color:'#0078D4',bg:'#fff',   border:'#0078D4'};
  };

  const tableBuilds=(myFiles||[]).filter(f=>f.source==='table_build');

  return(
    <div style={{flex:1,display:'flex',flexDirection:'column',alignItems:'center',padding:'28px 24px 48px',overflowY:'auto'}}>
      {/* Column Mapping Modal */}
      {colMapping&&(
        <div style={{position:'fixed',inset:0,zIndex:9999,background:'rgba(0,0,0,0.5)',display:'flex',alignItems:'center',justifyContent:'center',padding:16}} onClick={()=>setColMapping(null)}>
          <div style={{background:'#fff',borderRadius:12,width:'100%',maxWidth:700,maxHeight:'88vh',display:'flex',flexDirection:'column',boxShadow:'0 24px 64px rgba(0,0,0,0.35)'}} onClick={e=>e.stopPropagation()}>
            <div style={{padding:'20px 24px 16px',borderBottom:'1px solid #E2E8F0',flexShrink:0}}>
              <div style={{fontSize:10,fontWeight:700,color:'#DC2626',textTransform:'uppercase',letterSpacing:1,marginBottom:4}}>Column Mapping</div>
              <div style={{fontSize:17,fontWeight:700,color:'#1e293b'}}>Map Columns for {colMapping.tableDef.name}</div>
              <div style={{fontSize:12,color:'#64748b',marginTop:3}}>Select which columns from your file correspond to the required fields.</div>
            </div>
            <div style={{overflowY:'auto',flex:1,padding:'0 0 8px'}}>
              <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
                <thead style={{position:'sticky',top:0,zIndex:2}}>
                  <tr style={{background:'#F8FAFC'}}>
                    <th style={{padding:'10px 16px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',borderBottom:'2px solid #E2E8F0',width:'35%'}}>Required Column</th>
                    <th style={{padding:'10px 12px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',borderBottom:'2px solid #E2E8F0',width:'35%'}}>Map to File Column</th>
                    <th style={{padding:'10px 12px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',borderBottom:'2px solid #E2E8F0'}}>Purpose</th>
                  </tr>
                </thead>
                <tbody>
                  {colMapping.tableDef.required.map((r)=>{
                    const reqCol=r.col;
                    const autoMatch=colMapping.uploadedCols.find(c=>c.toUpperCase()===reqCol.toUpperCase());
                    const selected=colMapping.mapping[reqCol]!==undefined?colMapping.mapping[reqCol]:(autoMatch||'');
                    return(
                      <tr key={reqCol} style={{borderBottom:'1px solid #F1F5F9',background:'#fff'}}>
                        <td style={{padding:'10px 16px',fontFamily:'monospace',fontWeight:700,color:'#334155',fontSize:13}}>{reqCol}</td>
                        <td style={{padding:'10px 12px'}}>
                          <select value={selected} onChange={e=>setColMapping(p=>({...p,mapping:{...p.mapping,[reqCol]:e.target.value}}))} style={{width:'100%',padding:'6px 8px',borderRadius:4,border:'1px solid #CBD5E1',background:'#fff',fontSize:12,color:'#334155'}}>
                            <option value="">-- Leave Blank / Unmapped --</option>
                            {colMapping.uploadedCols.map(c=>(<option key={c} value={c}>{c}</option>))}
                          </select>
                        </td>
                        <td style={{padding:'10px 12px',color:'#64748b',fontSize:11,lineHeight:1.4}}>{r.note}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <div style={{padding:'14px 20px',borderTop:'1px solid #E2E8F0',flexShrink:0,display:'flex',justifyContent:'flex-end',gap:12}}>
              <button onClick={()=>setColMapping(null)} style={{padding:'8px 16px',background:'#fff',color:'#64748b',border:'1px solid #CBD5E1',borderRadius:6,fontSize:13,fontWeight:700,cursor:'pointer'}}>Cancel</button>
              <button onClick={async()=>{
                const finalMapping={};
                colMapping.tableDef.required.forEach(r=>{
                  const autoMatch=colMapping.uploadedCols.find(c=>c.toUpperCase()===r.col.toUpperCase());
                  const sel=colMapping.mapping[r.col]!==undefined?colMapping.mapping[r.col]:(autoMatch||'');
                  if(sel) finalMapping[sel]=r.col;
                });
                await fetch(`${API}/p2i/transform/clear_table?table_name=${colMapping.tableDef.name}&username=${encodeURIComponent(currentUser||'Unknown')}`,{method:'DELETE'}).catch(console.error);
                setAppliedMappings(p=>({...p,[colMapping.tableDef.name]:finalMapping}));
                performUpload(colMapping.tableDef.name,colMapping.file,finalMapping);
                setColMapping(null);
              }} style={{padding:'8px 16px',background:'#038387',color:'#fff',border:'none',borderRadius:6,fontSize:13,fontWeight:700,cursor:'pointer'}}>
                Confirm Mapping & Upload
              </button>
            </div>
          </div>
        </div>
      )}

      <div style={{maxWidth:820,width:'100%',display:'flex',flexDirection:'column',gap:20}}>
        <div style={{display:'flex',alignItems:'center',gap:12}}>
          <button onClick={onBack} style={{background:'none',border:'1px solid #E2E8F0',padding:'6px 14px',borderRadius:6,fontSize:12,cursor:'pointer',color:'#64748b',fontWeight:600}}>← Back</button>
          <div>
            <div style={{fontSize:11,fontWeight:700,color:'#038387',textTransform:'uppercase',letterSpacing:0.8}}>Build Event Log</div>
            <div style={{fontSize:13,color:'#64748b'}}>Upload SAP tables below, then click Build. AFKO and AFPO are mandatory.</div>
          </div>
        </div>

        <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 22px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:14}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8}}>SAP Tables</div>
            <div style={{fontSize:11,color:'#94a3b8'}}>Upload each as <strong style={{color:'#475569'}}>.csv</strong></div>
          </div>
          <div style={{display:'flex',flexDirection:'column',border:'1px solid #E2E8F0',borderRadius:8,overflow:'hidden'}}>
            {tables.map((t,i)=>{
              const s=si(tableStatus[t.name]);
              const ref=fileRefs.current[t.name];
              const isUp=tableStatus[t.name]==='uploading';
              return(
                <div key={t.name} style={{display:'flex',alignItems:'center',gap:12,padding:'10px 14px',background:tableStatus[t.name]==='done'?'#F0FAF0':tableStatus[t.name]==='error'?'#FDE7E9':i%2===0?'#F8FAFC':'#fff',borderBottom:i<tables.length-1?'1px solid #E2E8F0':'none',transition:'background 0.2s'}}>
                  <input ref={ref} type="file" accept=".csv" style={{display:'none'}} onChange={e=>{const f=e.target.files[0];e.target.value='';if(f) uploadTable(t.name,f);}}/>
                  <button onClick={()=>{if(!isUp&&ref.current){ref.current.value='';ref.current.click();}}} disabled={isUp}
                    style={{display:'flex',alignItems:'center',justifyContent:'center',width:28,height:28,borderRadius:6,border:`1.5px solid ${s.border}`,background:s.bg,color:s.color,cursor:isUp?'not-allowed':'pointer',fontWeight:700,fontSize:13,flexShrink:0}}>
                    {isUp?<span style={{animation:'spin 1s linear infinite',display:'inline-block'}}>↻</span>:s.icon}
                  </button>
                  <div style={{display:'flex',alignItems:'center',gap:8,minWidth:52,flexShrink:0}}>
                    <div style={{fontFamily:'monospace',fontWeight:700,fontSize:13,color:'#038387',background:'#F0FDFA',padding:'3px 8px',borderRadius:4,textAlign:'center'}}>{t.name}</div>
                    {t.isMandatory&&<span style={{fontSize:9,background:'#E0F2FE',color:'#0369a1',padding:'2px 5px',borderRadius:4,fontWeight:700}}>REQ</span>}
                  </div>
                  <div style={{fontSize:13,color:'#475569',flex:1}}>
                    {t.desc}
                    {appliedMappings[t.name]&&Object.keys(appliedMappings[t.name]).length>0&&(
                      <div style={{fontSize:11,color:'#038387',marginTop:4,display:'flex',flexWrap:'wrap',gap:6}}>
                        {Object.entries(appliedMappings[t.name]).map(([k,v])=>(<span key={k} style={{background:'#F0FDFA',padding:'2px 6px',borderRadius:4}}><strong>{k}</strong> → {v}</span>))}
                      </div>
                    )}
                  </div>
                  <div style={{display:'flex',alignItems:'center',gap:5,marginLeft:'auto',flexShrink:0}}>
                    {tableMsg[t.name]&&(
                      <div style={{fontSize:11,fontWeight:600,maxWidth:260,lineHeight:1.3,color:tableStatus[t.name]==='error'?'#DC2626':'#15803D',background:tableStatus[t.name]==='error'?'#FEF2F2':'transparent',padding:tableStatus[t.name]==='error'?'3px 6px':'0',borderRadius:4,border:tableStatus[t.name]==='error'?'1px solid #FECACA':'none'}}>
                        {tableMsg[t.name]}
                      </div>
                    )}
                    {(tableStatus[t.name]==='done'||tableStatus[t.name]==='error')&&(
                      <div style={{display:'flex',gap:4,alignItems:'center'}}>
                        {selectedFiles[t.name]&&(
                          <button onClick={()=>handleMapColumns(t.name)} title="Map columns"
                            style={{fontSize:10,fontWeight:700,padding:'2px 8px',borderRadius:4,background:tableStatus[t.name]==='error'?'#FEF2F2':'#EFF6FF',color:tableStatus[t.name]==='error'?'#DC2626':'#1D4ED8',border:tableStatus[t.name]==='error'?'1px solid #FCA5A5':'1px solid #93C5FD',cursor:'pointer'}}>
                            Map Columns
                          </button>
                        )}
                        <button onClick={()=>{
                          fetch(`${API}/p2i/transform/clear_table?table_name=${t.name}&username=${encodeURIComponent(currentUser||'Unknown')}`,{method:'DELETE'}).catch(console.error);
                          setTableStatus(p=>({...p,[t.name]:'idle'}));setTableMsg(p=>({...p,[t.name]:''}));
                          setSelectedFiles(p=>{const copy={...p};delete copy[t.name];return copy;});
                          setAppliedMappings(p=>{const copy={...p};delete copy[t.name];return copy;});
                          if(fileRefs.current[t.name]?.current) fileRefs.current[t.name].current.value='';
                        }} title="Clear to re-upload"
                          style={{display:'flex',alignItems:'center',justifyContent:'center',width:18,height:18,borderRadius:'50%',border:'1.5px solid #FCA5A5',background:'#FEE2E2',color:'#DC2626',cursor:'pointer',fontWeight:800,fontSize:10,padding:0,lineHeight:1,flexShrink:0}}>
                          ✕
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
          <div style={{marginTop:16,display:'flex',alignItems:'center',gap:12,justifyContent:'space-between',flexWrap:'wrap'}}>
            <div style={{fontSize:12,color:allDone?'#107C10':'#94a3b8',fontWeight:allDone?700:400}}>
              {allDone?`✓ ${tables.filter(t=>tableStatus[t.name]==='done').length} table(s) uploaded — ready to build`:`${tables.filter(t=>tableStatus[t.name]==='done').length} / ${tables.length} tables uploaded (need at least AFKO or AFPO)`}
            </div>
            <div style={{display:'flex',alignItems:'center',gap:10,flexWrap:'wrap'}}>
              {buildMsg&&<div style={{fontSize:12,color:buildMsg.startsWith('Error')?'#D13438':'#107C10',fontWeight:600}}>{buildMsg}</div>}
              <button onClick={handleClearAll} disabled={anyUploading}
                style={{background:'#fff',color:'#D13438',border:'1px solid #D13438',padding:'9px 18px',borderRadius:6,fontSize:13,fontWeight:700,cursor:anyUploading?'not-allowed':'pointer',whiteSpace:'nowrap'}}>
                Clear All
              </button>
              <button onClick={handleBuild} disabled={!allDone||anyUploading}
                style={{background:allDone&&!anyUploading?'#038387':'#A8A8A8',color:'#fff',border:'none',padding:'10px 28px',borderRadius:6,fontSize:13,fontWeight:700,cursor:allDone&&!anyUploading?'pointer':'not-allowed',whiteSpace:'nowrap',boxShadow:allDone&&!anyUploading?'0 2px 8px rgba(3,131,135,0.3)':'none'}}
                onMouseOver={e=>{if(allDone&&!anyUploading)e.currentTarget.style.background='#026769';}}
                onMouseOut={e=>{e.currentTarget.style.background=allDone&&!anyUploading?'#038387':'#A8A8A8';}}>
                {building?'⏳ Building…':'Build Event Log'}
              </button>
            </div>
          </div>
        </div>

        {/* Previous Table Builds */}
        <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'18px 20px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:12}}>Previous Table Builds</div>
          {fetchingFiles?(
            <div style={{color:'#94a3b8',fontSize:13}}>Loading...</div>
          ):tableBuilds.length===0?(
            <div style={{padding:'20px',textAlign:'center',background:'#F8FAFC',borderRadius:8,border:'1px dashed #E2E8F0',color:'#94a3b8',fontSize:13}}>
              No previous builds found. Upload tables above and click Build.
            </div>
          ):(
            <div style={{border:'1px solid #E2E8F0',borderRadius:8,overflow:'hidden'}}>
              <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
                <thead style={{background:'#F3F2F1',borderBottom:'1px solid #E2E8F0',textAlign:'left'}}>
                  <tr>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Name</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Date</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600,textAlign:'right'}}>Rows</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {tableBuilds.map((f,idx)=>(
                    <tr key={idx} style={{borderBottom:'1px solid #E2E8F0',transition:'background 0.2s'}} onMouseEnter={e=>e.currentTarget.style.background='#F8FAFC'} onMouseLeave={e=>e.currentTarget.style.background='transparent'}>
                      <td style={{padding:'9px 14px'}}>
                        <div style={{fontWeight:600,color:'#1e293b',fontSize:12}}>{f.name}</div>
                        <span style={{fontSize:9,fontWeight:700,padding:'2px 7px',borderRadius:10,background:'#F0FDFA',color:'#0f766e',border:'1px solid #99f6e4'}}>Table Build</span>
                      </td>
                      <td style={{padding:'9px 14px',color:'#64748b',whiteSpace:'nowrap',fontSize:11}}>{new Date(f.ts*1000).toLocaleDateString()}</td>
                      <td style={{padding:'9px 14px',color:'#64748b',textAlign:'right'}}>—</td>
                      <td style={{padding:'9px 14px'}}>
                        <button onClick={()=>handleLoadOldFile&&handleLoadOldFile(f.id)}
                          style={{background:'#038387',color:'#fff',border:'none',padding:'5px 12px',borderRadius:4,cursor:'pointer',fontSize:11,fontWeight:600}}
                          onMouseOver={e=>e.currentTarget.style.background='#026769'} onMouseOut={e=>e.currentTarget.style.background='#038387'}>
                          Load Dashboard
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

/* ─── UPLOAD BANNER ─── */
const UploadBanner=React.memo(({onUploaded,serverOk,onLoadingChange,currentUser,myFiles,fetchingFiles,handleLoadOldFile,defaultStep})=>{
  const [step,setStep]=useState(defaultStep||'info');
  const [dragging,setDragging]=useState(false);
  const [status,setStatus]=useState('idle');
  const [msg,setMsg]=useState('');
  const [selectedFile,setSelectedFile]=useState(null);
  const inputRef=useRef();

  const doUpload=async(file,mapping={})=>{
    if(!file) return;
    if(!file.name.toLowerCase().endsWith('.csv')){setStatus('error');setMsg('Only .csv files accepted.');return;}
    setStatus('uploading');setMsg('');
    onLoadingChange(true,10,'Processing Data...');
    const form=new FormData();
    form.append('file',file);form.append('username',currentUser);form.append('column_mapping',JSON.stringify(mapping));
    let prog=10;
    const ticker=setInterval(()=>{prog=Math.min(prog+Math.random()*12,88);onLoadingChange(true,prog,'Analysing Data...');},400);
    try{
      const r=await fetch(`${API}/p2i/upload`,{method:'POST',body:form});
      const d=await r.json();
      clearInterval(ticker);
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      onLoadingChange(true,100,'Dashboard Created');
      setTimeout(()=>{setStatus('done');setMsg(`✓ ${Number(d.rows).toLocaleString()} rows loaded`);onUploaded(null,'upload');},800);
    }catch(e){
      clearInterval(ticker);onLoadingChange(false,0,'');
      setStatus('error');setMsg(`Error: ${e.message}`);
      onUploaded(e.message,'upload');
    }finally{
      if(inputRef.current) inputRef.current.value='';
    }
  };

  if(step==='info') return(
    <P2IIntroScreen onGoTableBuild={()=>setStep('table')} onGoCsvUpload={()=>setStep('upload')} currentUser={currentUser}/>
  );

  if(step==='table') return(
    <TableUploadScreen onBuilt={onUploaded} onBack={()=>setStep('info')} onLoadingChange={onLoadingChange} currentUser={currentUser} myFiles={myFiles} fetchingFiles={fetchingFiles} handleLoadOldFile={handleLoadOldFile}/>
  );

  /* Pre-built CSV upload */
  const bc=dragging?C.teal:status==='done'?'#107C10':status==='error'?C.red:C.border;
  const bg=dragging?'#F0FDFA':status==='done'?'#F0FAF0':status==='error'?'#FDE7E9':'#FAFAFA';
  const csvUploads=(myFiles||[]).filter(f=>!f.source||f.source==='upload');

  return(
    <div style={{display:'flex',flexDirection:'column',gap:16,padding:'20px 14px'}}>
      <div style={{display:'flex',alignItems:'center',gap:10}}>
        <button onClick={()=>setStep('info')} style={{background:'none',border:'1px solid #E2E8F0',padding:'5px 12px',borderRadius:6,fontSize:12,cursor:'pointer',color:'#64748b',fontWeight:600,flexShrink:0}}>← Back</button>
        <div style={{fontSize:11,fontWeight:700,color:'#038387',textTransform:'uppercase',letterSpacing:0.8}}>Upload Pre-built CSV</div>
        <div style={{display:'flex',alignItems:'center',gap:5,fontSize:11,color:C.slate,marginLeft:'auto'}}>
          <div style={{width:7,height:7,borderRadius:'50%',background:serverOk?'#107C10':'#D13438',boxShadow:serverOk?'0 0 0 2px rgba(16,124,16,.2)':'0 0 0 2px rgba(209,52,56,.2)'}}/>
          {serverOk?'Backend connected':'Backend offline'}
        </div>
      </div>

      <div onDragOver={e=>{e.preventDefault();setDragging(true);}} onDragLeave={()=>setDragging(false)}
        onDrop={e=>{e.preventDefault();setDragging(false);const f=e.dataTransfer.files[0];if(f){setSelectedFile(f);setStatus('idle');setMsg('');}}}
        onClick={()=>{if(status!=='uploading'&&inputRef.current){inputRef.current.value='';inputRef.current.click();}}}
        style={{border:`2px dashed ${bc}`,borderRadius:8,padding:'14px 24px',background:bg,cursor:'pointer',textAlign:'center',transition:'all .2s',display:'flex',alignItems:'center',justifyContent:'center',gap:14,flexDirection:selectedFile?'column':'row'}}>
        <input ref={inputRef} type="file" accept=".csv" style={{display:'none'}} onChange={e=>{const f=e.target.files[0];if(f){setSelectedFile(f);setStatus('idle');setMsg('');}}}/>
        <div style={{display:'flex',alignItems:'center',gap:14}}>
          <div style={{fontSize:22,fontWeight:'bold',color:status==='done'?'#107C10':status==='error'?'#D13438':'#038387'}}>{status==='done'?'✓':status==='error'?'✕':'⬆'}</div>
          <div style={{textAlign:'left'}}>
            <div style={{fontSize:13,fontWeight:700,color:'#323130'}}>{selectedFile?selectedFile.name:status==='idle'?'Click or drag & drop a CSV file here':status==='done'?'File loaded!':'Upload failed'}</div>
            <div style={{fontSize:11,color:C.slate,marginTop:2}}>{msg||'Formatted event log CSV accepted (Case ID, Activity, Timestamp)'}</div>
          </div>
        </div>
        {selectedFile&&status!=='uploading'&&(
          <div style={{display:'flex',gap:12,marginTop:4}}>
            <button onClick={e=>{e.stopPropagation();doUpload(selectedFile,{});}}
              style={{fontSize:12,padding:'8px 16px',background:'#038387',color:'#fff',border:'none',borderRadius:6,cursor:'pointer',fontWeight:700}}>
              Upload
            </button>
            <button onClick={e=>{e.stopPropagation();setStatus('idle');setMsg('');setSelectedFile(null);}}
              style={{fontSize:12,padding:'8px 16px',background:'#fff',border:`1px solid ${C.border}`,borderRadius:6,cursor:'pointer',color:C.slate}}>
              Clear
            </button>
          </div>
        )}
      </div>

      {/* Previous CSV Uploads */}
      <div style={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:10,padding:'18px 20px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
        <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:12}}>Previous CSV Uploads</div>
        {fetchingFiles?(
          <div style={{color:'#94a3b8',fontSize:13}}>Loading...</div>
        ):csvUploads.length===0?(
          <div style={{padding:'20px',textAlign:'center',background:'#F8FAFC',borderRadius:8,border:'1px dashed #E2E8F0',color:'#94a3b8',fontSize:13}}>No previous CSV uploads found.</div>
        ):(
          <div style={{border:'1px solid #E2E8F0',borderRadius:8,overflow:'hidden'}}>
            <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
              <thead style={{background:'#F3F2F1',borderBottom:'1px solid #E2E8F0',textAlign:'left'}}>
                <tr>
                  <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>File Name</th>
                  <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Date</th>
                  <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Action</th>
                </tr>
              </thead>
              <tbody>
                {csvUploads.map((f,idx)=>(
                  <tr key={idx} style={{borderBottom:'1px solid #E2E8F0',transition:'background 0.2s'}} onMouseEnter={e=>e.currentTarget.style.background='#F8FAFC'} onMouseLeave={e=>e.currentTarget.style.background='transparent'}>
                    <td style={{padding:'9px 14px'}}>
                      <div style={{fontWeight:600,color:'#1e293b',fontSize:12}}>{f.name}</div>
                      <span style={{fontSize:9,fontWeight:700,padding:'2px 7px',borderRadius:10,background:'#EFF6FF',color:'#0057B7',border:'1px solid #B3D1F5'}}>CSV Upload</span>
                    </td>
                    <td style={{padding:'9px 14px',color:'#64748b',whiteSpace:'nowrap',fontSize:11}}>{new Date(f.ts*1000).toLocaleDateString()}</td>
                    <td style={{padding:'9px 14px'}}>
                      <button onClick={()=>handleLoadOldFile&&handleLoadOldFile(f.id)}
                        style={{background:'#038387',color:'#fff',border:'none',padding:'5px 12px',borderRadius:4,cursor:'pointer',fontSize:11,fontWeight:600}}
                        onMouseOver={e=>e.currentTarget.style.background='#026769'} onMouseOut={e=>e.currentTarget.style.background='#038387'}>
                        Load Dashboard
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
});

/* ════════════════════════════════════════════
   MAIN P2I DASHBOARD
════════════════════════════════════════════ */
export default function P2IDashboard({currentUser,onSignOut,onBackHome}){
  const [serverOk,setServerOk]=useState(false);
  const [dataLoaded,setDataLoaded]=useState(false);
  const [mappingError,setMappingError]=useState(null);
  const [loading,setLoading]=useState(false);
  const [dashboardLoading,setDashboardLoading]=useState(false);
  const [chartsReady,setChartsReady]=useState(false);
  const intentToUpload=useRef(true);
  const [loadProg,setLoadProg]=useState(0);
  const [loadLabel,setLoadLabel]=useState('');

  const [activeTab,setActiveTab]=useState('process');
  const [layoutDir,setLayoutDir]=useState('TB');

  const [selected,setSelected]=useState({order_id:'ALL',mtart:'ALL',plant:'ALL',year:'ALL',month:'ALL'});
  const [filters,setFilters]=useState({order_ids:[],mtarts:[],plants:[],years:[],months:[]});
  const [crossFilter,setCrossFilter]=useState(null);

  const [kpis,setKpis]=useState(null);
  const [ltData,setLtData]=useState([]);
  const [devData,setDevData]=useState([]);
  const [monthlyData,setMonthlyData]=useState([]);
  const [plantData,setPlantData]=useState([]);
  const [rfNodes,setRfNodes,onNodesChange]=useNodesState([]);
  const [rfEdges,setRfEdges,onEdgesChange]=useEdgesState([]);
  const [pmLoading,setPmLoading]=useState(false);
  const [pmReady,setPmReady]=useState(false);
  const [rawGraphData,setRawGraphData]=useState(null);

  const [refreshTrigger,setRefreshTrigger]=useState(0);
  const [myFiles,setMyFiles]=useState([]);
  const [fetchingFiles,setFetchingFiles]=useState(false);
  const [uploadStepOverride,setUploadStepOverride]=useState(null);

  useEffect(()=>{
    if(currentUser&&!dataLoaded){
      setFetchingFiles(true);
      fetch(`${API}/p2i/my_files?username=${currentUser}`)
        .then(res=>res.ok?res.json():[])
        .then(data=>setMyFiles(data))
        .catch(err=>console.error('Failed to fetch files',err))
        .finally(()=>setFetchingFiles(false));
    }
  },[currentUser,dataLoaded,refreshTrigger]);

  const handleLoadOldFile=async(file_id)=>{
    handleLoadingChange(true,50,'Loading previous dashboard...');
    try{
      const res=await fetch(`${API}/p2i/load_file`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:currentUser,file_id})});
      if(!res.ok) throw new Error('Failed to load file');
      intentToUpload.current=false;
      setDataLoaded(true);
      setRefreshTrigger(p=>p+1);
    }catch(e){alert('Error loading dashboard: '+e.message);}
    finally{setTimeout(()=>handleLoadingChange(false,100,''),500);}
  };

  const handleLoadingChange=useCallback((vis,prog,lbl)=>{setLoading(vis);setLoadProg(prog);setLoadLabel(lbl);},[]);

  const onUploaded=(err,source)=>{
    if(source) setUploadStepOverride(source);
    setMappingError(err||null);
    intentToUpload.current=false;
    setDataLoaded(true);
    setRefreshTrigger(p=>p+1);
  };

  const handleSignOut=async()=>{
    intentToUpload.current=true;
    setDataLoaded(false);setChartsReady(false);setPmReady(false);setKpis(null);
    if(onSignOut) onSignOut();
  };

  const handleFixMapping=(sourceType)=>{
    intentToUpload.current=true;
    setDataLoaded(false);setChartsReady(false);setPmReady(false);
    setUploadStepOverride(sourceType||'table');
  };

  const handleResetData=()=>{
    setMappingError(null);
    intentToUpload.current=true;
    setDataLoaded(false);setChartsReady(false);setPmReady(false);
    setUploadStepOverride(null);setKpis(null);
    setSelected({order_id:'ALL',mtart:'ALL',plant:'ALL',year:'ALL',month:'ALL'});
    setCrossFilter(null);
  };

  const handleRefresh=()=>setRefreshTrigger(p=>p+1);

  useEffect(()=>{
    if(!currentUser) return;
    const ping=()=>fetch(`${API}/`).then(r=>r.ok?r.json():null).then(d=>{setServerOk(!!(d?.status));}).catch(()=>setServerOk(false));
    ping();const t=setInterval(ping,5000);return()=>clearInterval(t);
  },[currentUser]);

  const getQs=useCallback(()=>{
    const p={...selected,username:currentUser};
    if (crossFilter && crossFilter.type && crossFilter.value) {
      p[crossFilter.type] = crossFilter.value;
    }
    const params=Object.entries(p).filter(([k,v])=>v&&v!=='ALL').map(([k,v])=>`${k}=${encodeURIComponent(v)}`);
    const userParam=`username=${encodeURIComponent(currentUser||'Unknown')}`;
    const has=params.some(p=>p.startsWith('username'));
    if(!has) params.push(userParam);
    return params.length?'?'+params.join('&'):`?${userParam}`;
  },[selected,currentUser,crossFilter]);

  useEffect(()=>{
    if(!dataLoaded) return;
    fetch(`${API}/p2i/filters?username=${encodeURIComponent(currentUser||'Unknown')}`)
      .then(r=>r.ok?r.json():{})
      .then(d=>setFilters(d&&typeof d==='object'&&!Array.isArray(d)?d:{}))
      .catch(()=>setFilters({}));
  },[dataLoaded,refreshTrigger,currentUser]);

  useEffect(()=>{
    if(!dataLoaded) return;
    if(chartsReady) setDashboardLoading(true);
    const qs=getQs();
    const arr=(u,s)=>fetch(u).then(r=>r.ok?r.json():[]).then(d=>s(Array.isArray(d)?d:[])).catch(()=>s([]));
    const obj=(u,s)=>fetch(u).then(r=>r.ok?r.json():null).then(d=>s(d&&typeof d==='object'&&!Array.isArray(d)?d:null)).catch(()=>s(null));

    const promises=[
      fetch(`${API}/p2i/kpis${qs}`).then(r=>{if(!r.ok)return{total_cases:0};return r.json();}).then(d=>setKpis(d)).catch(()=>setKpis({total_cases:0})),
      arr(`${API}/p2i/charts/lead_time_mtart${qs}`,setLtData),
      arr(`${API}/p2i/charts/deviations${qs}`,setDevData),
    ];

    // Fetch optional charts if endpoints exist
    fetch(`${API}/p2i/charts/monthly${qs}`).then(r=>r.ok?r.json():[]).then(d=>setMonthlyData(Array.isArray(d)?d:[])).catch(()=>setMonthlyData([]));
    fetch(`${API}/p2i/charts/plant${qs}`).then(r=>r.ok?r.json():[]).then(d=>setPlantData(Array.isArray(d)?d:[])).catch(()=>setPlantData([]));

    Promise.all(promises).finally(()=>{setDashboardLoading(false);setChartsReady(true);});
  },[getQs,dataLoaded,refreshTrigger]);

  useEffect(()=>{
    if(!dataLoaded) return;
    const qs=getQs();
    if(pmReady) setPmLoading(true);
    fetch(`${API}/p2i/nodes_edges${qs}`)
      .then(r=>{if(!r.ok) throw new Error(`HTTP ${r.status}`);return r.json();})
      .then(d=>{
        setRawGraphData(d);
        buildP2IFlowMap(d.nodes,d.edges,setRfNodes,setRfEdges,layoutDir);
      })
      .catch(err=>console.error('PM error:',err))
      .finally(()=>{setPmLoading(false);setPmReady(true);});
  },[getQs,dataLoaded,refreshTrigger]);

  useEffect(()=>{
    if(rawGraphData) buildP2IFlowMap(rawGraphData.nodes,rawGraphData.edges,setRfNodes,setRfEdges,layoutDir);
  },[layoutDir,rawGraphData]);

  useEffect(()=>{
    if(dataLoaded&&chartsReady&&pmReady&&loading) setLoading(false);
  },[dataLoaded,chartsReady,pmReady,loading]);

  const slicer=(key,label,filterKey)=>{
    const raw=filters[filterKey];
    const opts=Array.isArray(raw)?raw:[];
    const deduped=opts.filter(o=>o!=='ALL');
    if(key==='order_id') return <SearchableSelect key={key} label={label} value={selected[key]||'ALL'} options={deduped} onChange={val=>{setSelected(prev=>({...prev,[key]:val}));setCrossFilter(null);}}/>;
    return(
      <FilterSelect key={key} label={label} value={selected[key]||'ALL'} options={['ALL',...deduped]}
        onChange={val=>{setSelected(prev=>({...prev,[key]:val}));setCrossFilter(null);}}/>
    );
  };

  const handleSelect=useCallback((type,value)=>{
    setCrossFilter(prev=>{
      const isRemoving=prev?.type===type&&prev?.value===value;
      return isRemoving?null:{type,value};
    });
  },[]);

  const clearCF=useCallback(()=>setCrossFilter(null),[]);

  const resetAll=()=>{
    setSelected({order_id:'ALL',mtart:'ALL',plant:'ALL',year:'ALL',month:'ALL'});
    setCrossFilter(null);
  };

  return(
    <div style={{fontFamily:"'Segoe UI',-apple-system,sans-serif",background:C.bg,height:'100vh',display:'flex',flexDirection:'column'}}>
      <style>{`
        @keyframes spin{to{transform:rotate(360deg)}}
        @keyframes fadeIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
        .fade-in{animation:fadeIn 0.4s ease forwards}
        *{box-sizing:border-box}
        ::-webkit-scrollbar{width:5px;height:5px}
        ::-webkit-scrollbar-track{background:transparent}
        ::-webkit-scrollbar-thumb{background:#D2D0CE;border-radius:3px}
        ::-webkit-scrollbar-thumb:hover{background:#A19F9D}
        .tab-content{transition: opacity 0.3s ease; width: 100%;}
      `}</style>

      <LoadingOverlay visible={loading} progress={loadProg} label={loadLabel}/>

      {/* Header */}
      <div style={{background:C.headerBg,padding:'10px 20px',flexShrink:0,display:'flex',justifyContent:'space-between',alignItems:'center',boxShadow:'0 2px 8px rgba(0,0,0,.2)'}}>
        <div style={{display:'flex',alignItems:'center',gap:12}}>
          <img src="/logo.png" alt="AJALabs Logo" onClick={()=>onBackHome&&onBackHome()} title="Back to Home"
            style={{height:'36px',objectFit:'contain',cursor:'pointer',borderRadius:4,transition:'opacity 0.2s'}}
            onMouseOver={e=>{e.currentTarget.style.opacity='0.7';}} onMouseOut={e=>{e.currentTarget.style.opacity='1';}}/>
          <div>
            <div style={{fontWeight:700,fontSize:16,color:'#fff'}}>P2I Process Explorer</div>
            <div style={{fontSize:10,color:'rgba(255,255,255,.5)'}}>Plan-to-Inventory Process Mining</div>
          </div>
          {crossFilter&&(
            <div style={{display:'flex',alignItems:'center',gap:6,marginLeft:16,background:'rgba(255,255,255,.12)',border:'1px solid rgba(255,255,255,.2)',borderRadius:6,padding:'4px 12px',fontSize:12}}>
              <span style={{color:'#fff',fontWeight:600}}>Filter: {crossFilter.type}: <strong>{crossFilter.value}</strong></span>
              <button onClick={clearCF} style={{background:'none',border:'none',cursor:'pointer',color:'rgba(255,255,255,.8)',fontWeight:700,fontSize:14,padding:'0 2px'}}>X</button>
            </div>
          )}
        </div>

        <div style={{display:'flex',alignItems:'center',gap:12}}>
          {dataLoaded&&kpis&&(
            <div style={{fontSize:11,color:'rgba(255,255,255,.5)'}}>
              {Number(kpis.total_cases).toLocaleString()} orders loaded
            </div>
          )}

          {dataLoaded&&(
            <div style={{display:'flex',alignItems:'stretch',gap:0,background:'rgba(255,255,255,0.08)',borderRadius:6,border:'1px solid rgba(255,255,255,0.15)',overflow:'hidden'}}>
              <button className={`tab-button ${activeTab==='process'?'active':''}`} onClick={()=>setActiveTab('process')}>Process Mining</button>
              <button className={`tab-button ${activeTab==='eda'?'active':''}`} onClick={()=>setActiveTab('eda')}>EDA</button>
              <button onClick={handleResetData}
                style={{fontSize:11,fontWeight:600,background:'transparent',color:'rgba(255,255,255,0.75)',border:'none',borderLeft:'1px solid rgba(255,255,255,0.15)',padding:'8px 14px',cursor:'pointer',transition:'all 0.2s',whiteSpace:'nowrap'}}
                onMouseOver={e=>{e.currentTarget.style.background='rgba(255,255,255,0.12)';e.currentTarget.style.color='#fff';}}
                onMouseOut={e=>{e.currentTarget.style.background='transparent';e.currentTarget.style.color='rgba(255,255,255,0.75)';}}>
                📂 Upload New File
              </button>
            </div>
          )}

          <div style={{display:'flex',alignItems:'center',gap:12,marginLeft:16}}>
            <div style={{width:1,height:24,background:'rgba(255,255,255,0.2)'}}></div>
            <div style={{fontSize:12,color:'rgba(255,255,255,0.7)'}}>User: <strong style={{color:'#fff'}}>{currentUser}</strong></div>
            <button onClick={handleSignOut} style={{background:'rgba(209,52,56,0.85)',color:'#fff',border:'none',padding:'6px 12px',borderRadius:4,cursor:'pointer',fontSize:11,fontWeight:700,transition:'all 0.2s'}}
              onMouseOver={e=>e.currentTarget.style.background='#D13438'} onMouseOut={e=>e.currentTarget.style.background='rgba(209,52,56,0.85)'}>
              Sign Out
            </button>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div style={{flex:1,overflowY:'auto',padding:'12px 14px 40px',display:'flex',flexDirection:'column',gap:10}}>

        {!dataLoaded&&(
          <UploadBanner currentUser={currentUser} onUploaded={onUploaded} serverOk={serverOk} onLoadingChange={handleLoadingChange} myFiles={myFiles} fetchingFiles={fetchingFiles} handleLoadOldFile={handleLoadOldFile} defaultStep={uploadStepOverride}/>
        )}

        {dataLoaded&&(
          <div style={{paddingRight:10,fontSize:11,color:C.slate,fontWeight:600,textAlign:'right'}}>
            {crossFilter ? `Cross-filtering active: ${crossFilter.type}` : 'Interactive Analysis'}
          </div>
        )}

        {/* Mapping error warning */}
        {dataLoaded&&(mappingError||(kpis&&kpis.total_cases===0))&&(
          <div style={{background:'#FFF4CE',border:'1px solid #FDE7E9',borderRadius:8,padding:'12px 20px',display:'flex',alignItems:'center',justifyContent:'space-between',gap:16,boxShadow:'0 2px 4px rgba(0,0,0,0.05)',marginBottom:2}}>
            <div style={{display:'flex',alignItems:'center',gap:12}}>
              <span style={{fontSize:20}}>⚠️</span>
              <div>
                <div style={{fontWeight:700,fontSize:13,color:'#323130'}}>Column mapping might be wrong</div>
                <div style={{fontSize:12,color:'#605E5C',marginTop:2}}>{mappingError||'No production orders found. Please check your column mappings.'}</div>
              </div>
            </div>
            <button onClick={()=>handleFixMapping(uploadStepOverride||'table')}
              style={{padding:'8px 20px',background:C.blue700,color:'#fff',border:'none',borderRadius:6,fontWeight:700,cursor:'pointer',fontSize:12,transition:'all 0.2s'}}
              onMouseOver={e=>e.currentTarget.style.background='#005A9E'} onMouseOut={e=>e.currentTarget.style.background=C.blue700}>
              Fix Mapping
            </button>
          </div>
        )}

        {dataLoaded&&(<>
          {/* Slicers */}
          <div style={{background:C.card,borderRadius:8,padding:'10px 14px',border:`1px solid ${C.border}`,boxShadow:'0 2px 6px rgba(0,0,0,.04)'}}>
            <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit, minmax(140px, 1fr))',gap:'12px',alignItems:'end'}}>
              {slicer('order_id','Order ID','order_ids')}
              {slicer('mtart','Material Type','mtarts')}
              {slicer('plant','Plant (WERKS)','plants')}
              {slicer('year','Year','years')}
              {slicer('month','Month','months')}
              <div style={{display:'flex',gap:8}}>
                <button onClick={resetAll} style={{padding:'6px 12px',fontSize:12,fontWeight:700,background:'#F3F2F1',color:'#323130',border:`1px solid #D2D0CE`,borderRadius:4,cursor:'pointer',height:'28px',flex:1}}>Reset</button>
                <button onClick={handleRefresh} style={{padding:'6px 12px',fontSize:12,fontWeight:700,background:C.blue700,color:'#fff',border:'none',borderRadius:4,cursor:'pointer',height:'28px',flex:1,display:'flex',alignItems:'center',justifyContent:'center'}}>🔄</button>
              </div>
            </div>
          </div>

          {kpis && (
            <div style={{display:'flex', flexDirection:'column', gap:8}}>
              <div style={{display:'grid',gridTemplateColumns:'repeat(7,1fr)',gap:8}}>
                <KpiCard label="Production Orders" value={kpis.total_cases} tooltip="Total unique production orders"/>
                <KpiCard label="Avg Lead Time" value={kpis.avg_lead_time} suffix="d" tooltip="Avg days from Order Creation to FG Goods Receipt"/>
                <KpiCard label="Scrap Rate" value={kpis.scrap_rate} suffix="%" tooltip="% of orders with recorded scrap"/>
                <KpiCard label="Rework Rate" value={kpis.rework_rate} suffix="%" tooltip="% of orders requiring rework"/>
                <KpiCard label="TECO Orders" value={kpis.teco_cases} tooltip="Technically Completed orders"/>
                <KpiCard label="WIP Orders" value={kpis.total_cases-(kpis.teco_cases||0)} tooltip="Orders not yet technically complete"/>
                <KpiCard label="Over-Produced" value={kpis.over_prod_cases} tooltip="Orders where yield exceeded planned quantity"/>
              </div>
              <div style={{display:'grid',gridTemplateColumns:'repeat(2,1fr)',gap:8}}>
                <ConfKpiCard label="Over-Production Cases" value={kpis?.over_prod_cases} sub="Yield exceeded planned order quantity"/>
                <ConfKpiCard label="Material Delay Cases" value={kpis?.material_delay_cases} sub="GI posted after component reservation date"/>
              </div>
            </div>
          )}

          {/* Process Mining Tab */}
          {activeTab==='process'&&(
            <div className="fade-in" style={{display:'grid', gridTemplateColumns:'1.5fr 1fr', gap:10, paddingBottom:20}}>
              <div style={{display:'flex', flexDirection:'column', gap:10}}>
                <div style={{background:C.card,borderRadius:8,border:`1px solid ${C.border}`,boxShadow:'0 2px 8px rgba(0,0,0,.05)',overflow:'hidden',display:'flex',flexDirection:'column',height:700}}>
                  <div style={{padding:'12px 14px 8px',borderBottom:`1px solid ${C.border}`,background:C.jkBlue,display:'flex',justifyContent:'space-between',alignItems:'center',flexShrink:0}}>
                    <div>
                      <div style={{fontSize:13,fontWeight:700,color:'#fff'}}>Process Discovery Map</div>
                      <div style={{fontSize:10,color:'rgba(255,255,255,0.7)'}}>Unique Case Flow & Sequence Analysis</div>
                    </div>
                    <div style={{display:'flex',gap:4,background:'rgba(255,255,255,0.2)',padding:2,borderRadius:4}}>
                      <button onClick={()=>setLayoutDir('LR')} style={{fontSize:11,padding:'4px 8px',border:'none',cursor:'pointer',borderRadius:3,background:layoutDir==='LR'?'#fff':'transparent',color:layoutDir==='LR'?C.jkBlue:'#fff',fontWeight:layoutDir==='LR'?700:400}}>Horizontal</button>
                      <button onClick={()=>setLayoutDir('TB')} style={{fontSize:11,padding:'4px 8px',border:'none',cursor:'pointer',borderRadius:3,background:layoutDir==='TB'?'#fff':'transparent',color:layoutDir==='TB'?C.jkBlue:'#fff',fontWeight:layoutDir==='TB'?700:400}}>Vertical</button>
                    </div>
                  </div>
                  <div style={{flex:1,position:'relative'}}>
                    <ReactFlow nodes={rfNodes} edges={rfEdges} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} nodeTypes={nodeTypes} edgeTypes={edgeTypes} fitView minZoom={0.1}>
                      <Background color="#C8D3E8" gap={24} size={1} variant="dots"/>
                      <Controls showInteractive={false}/>
                      <MiniMap zoomable pannable nodeColor={C.mapNodeBg} maskColor="rgba(240,244,250,.85)"/>
                    </ReactFlow>
                  </div>
                </div>
                <ChartCard title="Monthly Production Trend" subtitle="Unique orders processed per month — Click to filter" loading={dashboardLoading} highlighted={crossFilter?.type==='month'} onClear={clearCF}>
                  <MonthlyTrendChart data={monthlyData} crossFilter={crossFilter} onSelect={handleSelect}/>
                </ChartCard>
              </div>
              <div style={{display:'flex', flexDirection:'column', gap:10}}>
                <ChartCard title="Orders by Plant" subtitle="Manufacturing sites — Click to filter" loading={dashboardLoading} highlighted={crossFilter?.type==='plant'} onClear={clearCF}>
                  <PlantBarChart data={plantData} crossFilter={crossFilter} onSelect={handleSelect}/>
                </ChartCard>
                <ChartCard title="Order Event Timeline" subtitle="Audit trail for selected order" loading={dashboardLoading}>
                  <OrderTimeline orderId={selected.order_id} username={currentUser}/>
                </ChartCard>
              </div>
            </div>
          )}

          {/* EDA Tab */}
          {activeTab==='eda'&&(
            <div className="fade-in" style={{display:'flex',flexDirection:'column',gap:10,paddingBottom:20}}>
              <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10}}>
                <ChartCard title="Deviations Breakdown" subtitle="Frequency of deviation types — Click to filter" loading={dashboardLoading} highlighted={crossFilter?.type==='deviation'} onClear={clearCF}>
                  <DeviationsChart data={devData} crossFilter={crossFilter} onSelect={handleSelect}/>
                </ChartCard>
                <ChartCard title="Lead Time Distribution" subtitle="Avg days per material category — Click to filter" loading={dashboardLoading} highlighted={crossFilter?.type==='mtart'} onClear={clearCF}>
                  <LeadTimeMtartChart data={ltData} crossFilter={crossFilter} onSelect={handleSelect}/>
                </ChartCard>
              </div>
            </div>
          )}
        </>)}
      </div>

      <div style={{textAlign:'center',fontSize:'12px',color:'#605E5C',padding:'10px 0',borderTop:'1px solid #E1DFDD',flexShrink:0,zIndex:100}}>
        ©2023 <a href="https://ajalabs.ai" target="_blank" rel="noopener noreferrer" style={{color:'#323130',textDecoration:'none',fontWeight:'bold'}}>ajalabs.ai</a> All rights reserved - <a href="#" style={{color:'#0078D4',textDecoration:'none'}}>Data Privacy</a>
      </div>
    </div>
  );
}