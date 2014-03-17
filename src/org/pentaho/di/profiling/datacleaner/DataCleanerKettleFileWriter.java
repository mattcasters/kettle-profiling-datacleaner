package org.pentaho.di.profiling.datacleaner;

import java.io.DataOutputStream;
import java.util.List;

import org.apache.commons.vfs.FileObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

public class DataCleanerKettleFileWriter extends RowAdapter {

  private TransMeta transMeta;
  private StepMeta stepMeta;

  private DataOutputStream outputStream;
  private Trans trans;
  private LogChannelInterface log;

  private String filename;

  private RowMetaInterface normalRowMeta;
  private boolean conversionRequired;

  private long rowsStaged;

  public DataCleanerKettleFileWriter( Trans trans, StepMeta stepMeta ) throws Exception {
    this.trans = trans;
    this.transMeta = trans.getTransMeta();
    this.stepMeta = stepMeta;
    this.log = trans.getLogChannel();
  }

  public void run() throws Exception {
    FileObject tempFile = KettleVFS.createTempFile( "datacleaner", ".kettlestream",
      System.getProperty( "java.io.tmpdir" ) );
    filename = KettleVFS.getFilename( tempFile );

    outputStream = new DataOutputStream( KettleVFS.getOutputStream( tempFile, false ) );
    log.logBasic( "DataCleaner temp file created: " + filename );

    RowMetaInterface rowMeta = transMeta.getStepFields( stepMeta );
    normalRowMeta = rowMeta.clone();

    conversionRequired = false;
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
      if ( valueMeta.getStorageType() != ValueMetaInterface.STORAGE_TYPE_NORMAL ) {
        conversionRequired = true;
        normalRowMeta.getValueMeta( i ).setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
      }
    }

    log.logBasic( "Opened an output stream to DataCleaner." );

    // Write the transformation name, the step name and the row metadata
    // first...
    //
    outputStream.writeUTF( transMeta.getName() );
    log.logBasic( "wrote the transformation name." );

    outputStream.writeUTF( stepMeta.getName() );
    log.logBasic( "wrote the step name." );

    normalRowMeta.writeMeta( outputStream );
    log.logBasic( "Wrote the row metadata" );

    // Add a row listener to the selected step...
    //
    List<StepInterface> steps = trans.findBaseSteps( stepMeta.getName() );

    // Just do one step copy for the time being...
    //
    StepInterface step = steps.get( 0 );
    rowsStaged = 0L;

    step.addRowListener( this );
    log.logBasic( "Added the row listener to step: " + step.toString() );

    // Now start the transformation...
    //
    trans.startThreads();
    log.logBasic( "Started the transformation to profile... waiting until the transformation has finished" );

    trans.waitUntilFinished();

    log.logBasic( "The transformation to profile finished, " + rowsStaged + " rows staged." );
  }

  public String getFilename() {
    return filename;
  }

  @Override
  public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    try {
      if ( conversionRequired ) {
        Object[] normalRow = RowDataUtil.allocateRowData( rowMeta.size() );
        for ( int i = 0; i < rowMeta.size(); i++ ) {
          ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
          if ( valueMeta.getStorageType() != ValueMetaInterface.STORAGE_TYPE_NORMAL ) {
            normalRow[i] = valueMeta.convertToNormalStorageType( row[i] );
          } else {
            normalRow[i] = valueMeta.cloneValueData( row[i] );
          }
        }
        normalRowMeta.writeData( outputStream, normalRow );
      } else {
        rowMeta.writeData( outputStream, row );
      }
      rowsStaged++;
    } catch ( KettleException e ) {
      throw new KettleStepException( e );
    }
  }

  public void close() throws Exception {
    outputStream.flush();
    outputStream.close();
  }

}
