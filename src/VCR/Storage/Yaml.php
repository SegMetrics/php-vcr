<?php

namespace VCR\Storage;

use Symfony\Component\Yaml\Dumper;
use Symfony\Component\Yaml\Parser;

/**
 * Yaml based storage for records.
 *
 * This storage can be iterated while keeping the memory consumption to the
 * amount of memory used by the largest record.
 */
class Yaml extends AbstractStorage
{
    /**
     * @var Parser yaml parser
     */
    protected $yamlParser;

    /**
     * @var Dumper yaml writer
     */
    protected $yamlDumper;

    /**
     * Array of Request MD5 Hashes and the byte to seek to in the file
     * This improves our lookup speed
     *      key => [
     *              request -> the full request
     *              byte_pos -> the position in the file that this request starts on
     *              ]
     * @var array
     */
    private $recordingLookupArray = null;

    /**
     * The beginning of the line we should start reading from next
     * @var null
     */
    private $latestBytePosition = null;

    /**
     * Cache for the current index of the recordingLookupArray
     * @var int
     */
    private $latestKeyPosition = 0;

    /**
     * Creates a new YAML based file store.
     *
     * @param string $cassettePath path to the cassette directory
     * @param string $cassetteName path to a file, will be created if not existing
     * @param Parser $parser       parser used to decode yaml
     * @param Dumper $dumper       dumper used to encode yaml
     */
    public function __construct($cassettePath, $cassetteName, Parser $parser = null, Dumper $dumper = null)
    {
        parent::__construct($cassettePath, $cassetteName, '');

        $this->yamlParser = $parser ?: new Parser();
        $this->yamlDumper = $dumper ?: new Dumper();
    }

    /**
     * {@inheritdoc}
     */
    public function storeRecording(array $recording): void
    {
        fseek($this->handle, -1, SEEK_END);
        fwrite($this->handle, "\n".$this->yamlDumper->dump([$recording], 4));
        fflush($this->handle);

        // Every time we write, we need to reset the lookup array
        $this->resetLookupCache();
    }

    /**
     * Returns the current record.
     *
     * @return array<string,mixed>|null parsed current record
     */
    public function current()
    {
        if(empty( $this->recordingLookupArray[ $this->position ] )){ return []; }

        $recording = $this->yamlParser->parse($this->readNextRecord( $this->recordingLookupArray[ $this->position ]['byte_pos'] ));

        return empty($recording[0]) ? null : $recording[0];
    }

    /**
     * Parses the next record.
     *
     * @return void
     */
    public function next()
    {
        $this->latestKeyPosition = ++$this->position;
    }

    /**
     * Generates our lookup table if we don't have one already
     */
    private function generateLookupTable()
    {
        $this->resetLookupCache();

        while (true) {
            $recording = $this->yamlParser->parse($this->readNextRecord());
            if(empty($recording[0])){ break; }

            $this->recordingLookupArray[] = [
                'request'  => $recording[0]['request'] ?? null,
                'byte_pos' => $this->latestBytePosition ?? 0,
            ];
            $this->latestBytePosition = null;
        }
        $this->resetFilePosition();
    }

    /**
     * Returns the next record in raw format.
     *
     * @param int $startByte
     * @return string next record in raw format
     */
    private function readNextRecord( $startByte = 0): string
    {
        if ($this->isEOF) {
            $this->isValidPosition = false;
        }

        $isInRecord = false;
        $recording = '';

        if($startByte !== 0){
            fseek($this->handle, $startByte, SEEK_SET);
        }

        while (false !== ($line = fgets($this->handle))) {
            $isNewArrayStart = 0 === strpos($line, '-');

            if ($isInRecord && $isNewArrayStart) {
                fseek($this->handle, -\strlen($line), SEEK_CUR);
                break;
            }

            if (!$isInRecord && $isNewArrayStart) {
                $isInRecord = true;

                // Set the latest Byte Position to be the beginning of the line
                $this->latestBytePosition = ftell($this->handle) - strlen($line);
            }

            if ($isInRecord) {
                $recording .= $line;
            }
        }

        if (false == $line) {
            $this->isEOF = true;
        }

        return $recording;
    }

    /**
     * Resets the storage to the beginning.
     *
     * @return void
     */
    public function rewind()
    {
        $this->resetFilePosition();
        $this->position = $this->latestKeyPosition;
    }

    /**
     * Set the file storage to the beginning
     */
    private function resetFilePosition()
    {
        rewind($this->handle);
        $this->isEOF = false;
        $this->isValidPosition = true;
    }

    /**
     * Restart the lookup cache
     */
    private function resetLookupCache()
    {
        $this->recordingLookupArray = null;
        $this->latestBytePosition = null;
    }

    /**
     * Returns true if the current record is valid.
     *
     * @return bool true if the current record is valid
     */
    public function valid()
    {
        // Generate our lookup array if it doesn't exist
        if($this->recordingLookupArray === null){
            $this->generateLookupTable();
        }

        return isset( $this->recordingLookupArray[ $this->position ] );
    }
}
